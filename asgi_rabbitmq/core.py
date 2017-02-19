import weakref
from functools import partial

import msgpack
from asgiref.base_layer import BaseChannelLayer
from channels.signals import worker_ready
from pika import SelectConnection, URLParameters
from pika.channel import Channel as AMQPChannel
from pika.spec import Basic, BasicProperties

try:
    from concurrent.futures import Future
except ImportError:
    from futures import Future

try:
    from threading import Thread, get_ident
except ImportError:
    from threading import Thread, _get_ident as get_ident

try:
    import queue
except ImportError:
    import Queue as queue


class LayerAMQPChannel(AMQPChannel):

    def __init__(self, *args, **kwargs):

        self.amqp_ref = kwargs.pop('amqp_ref')
        super(LayerAMQPChannel, self).__init__(*args, **kwargs)

    def _on_deliver(self, method_frame, header_frame, body):

        # TODO: Refactor to the context manager.
        amqp = self.amqp_ref()
        try:
            amqp.amqp_channel = amqp.channels.get(method_frame.channel_number)
            amqp.resolve = amqp.futures.get(amqp.amqp_channel)
            return super(LayerAMQPChannel, self)._on_deliver(
                method_frame, header_frame, body)
        except Exception as error:
            try:
                amqp.resolve.set_exception(error)
            except AttributeError:
                pass
        finally:
            try:
                del amqp.amqp_channel
            except AttributeError:
                pass
            try:
                del amqp.resolve
            except AttributeError:
                pass

    def _on_getok(self, method_frame, header_frame, body):

        # TODO: Refactor to the context manager.
        amqp = self.amqp_ref()
        try:
            amqp.amqp_channel = amqp.channels.get(method_frame.channel_number)
            amqp.resolve = amqp.futures.get(amqp.amqp_channel)
            return super(LayerAMQPChannel, self)._on_getok(
                method_frame,
                header_frame,
                body,
            )
        except Exception as error:
            try:
                amqp.resolve.set_exception(error)
            except AttributeError:
                pass
        finally:
            try:
                del amqp.amqp_channel
            except AttributeError:
                pass
            try:
                del amqp.resolve
            except AttributeError:
                pass


class LayerSelectConnection(SelectConnection):

    Channel = LayerAMQPChannel

    def __init__(self, *args, **kwargs):

        self.amqp_ref = kwargs.pop('amqp_ref')
        super(LayerSelectConnection, self).__init__(*args, **kwargs)

    def _create_channel(self, channel_number, on_open_callback):

        return self.Channel(
            self,
            channel_number,
            on_open_callback,
            amqp_ref=self.amqp_ref,
        )

    def _process_callbacks(self, frame_value):

        # TODO: Refactor to the context manager.
        amqp = self.amqp_ref()
        try:
            amqp.amqp_channel = amqp.channels.get(frame_value.channel_number)
            amqp.resolve = amqp.futures.get(amqp.amqp_channel)
            return super(LayerSelectConnection,
                         self)._process_callbacks(frame_value)
        except Exception as error:
            try:
                amqp.resolve.set_exception(error)
            except AttributeError:
                pass
        finally:
            try:
                del amqp.amqp_channel
            except AttributeError:
                pass
            try:
                del amqp.resolve
            except AttributeError:
                pass


class AMQP(object):

    dead_letters = 'dead-letters'
    Parameters = URLParameters
    Connection = LayerSelectConnection

    def __init__(self, url, expiry, group_expiry, capacity, channel_capacity,
                 method_calls):

        self.parameters = self.Parameters(url)
        self.connection = self.Connection(
            parameters=self.parameters,
            on_open_callback=self.on_connection_open,
            amqp_ref=weakref.ref(self),
        )
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.capacity = capacity
        self.channel_capacity = channel_capacity
        self.method_calls = method_calls
        self.channels = {}
        self.numbers = {}
        self.futures = {}

    # Connection handling.

    def run(self):

        self.connection.ioloop.start()

    def on_connection_open(self, connection):

        self.process(None, self.on_dead_letter_channel_open, Future())
        self.check_method_call()

    def check_method_call(self):

        try:
            ident, method, future = self.method_calls.get_nowait()
            self.process(ident, method, future)
        except queue.Empty:
            pass
        self.connection.add_timeout(0.01, self.check_method_call)

    def process(self, ident, method, future):

        if (ident in self.numbers and
                self.channels[self.numbers[ident]].is_open):
            self.futures[self.channels[self.numbers[ident]]] = future
            self.apply_with_context(ident, method)
            return
        self.connection.channel(
            partial(self.register_channel, ident, method, future),
        )

    def register_channel(self, ident, method, future, amqp_channel):

        self.numbers[ident] = amqp_channel.channel_number
        self.channels[amqp_channel.channel_number] = amqp_channel
        self.futures[amqp_channel] = future
        self.apply_with_context(ident, method)

    def apply_with_context(self, ident, method):
        try:
            self.amqp_channel = self.channels[self.numbers[ident]]
            self.resolve = self.futures[self.amqp_channel]
            method()
        except Exception as error:
            self.resolve.set_exception(error)
        finally:
            del self.amqp_channel
            del self.resolve

    # Utilities.

    def get_queue_name(self, channel):

        if '!' in channel:
            return channel.rsplit('!', 1)[-1]
        elif '?' in channel:
            return channel.rsplit('?', 1)[-1]
        else:
            return channel

    # Send.

    def send(self, channel, message):

        # FIXME: Avoid constant queue declaration.  Or at least try to
        # minimize its impact to system.
        queue = self.get_queue_name(channel)
        self.amqp_channel.queue_declare(
            partial(self.publish_message, channel, message),
            queue=queue,
            passive=True if '!' in channel or '?' in channel else False,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

    def publish_message(self, channel, message, method_frame):

        if method_frame.method.message_count >= self.capacity:
            self.resolve.set_exception(RabbitmqChannelLayer.ChannelFull())
            return
        queue = self.get_queue_name(channel)
        body = self.serialize(message)
        self.amqp_channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=body,
            properties=self.publish_properties,
        )
        self.resolve.set_result(None)

    @property
    def publish_properties(self):

        expiration = str(self.expiry * 1000)
        properties = BasicProperties(expiration=expiration)
        return properties

    # Declare channel.

    def declare_channel(self, channel):

        self.amqp_channel.queue_declare(
            self.channel_declared,
            queue=channel,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

    def channel_declared(self, method_frame):

        self.resolve.set_result(None)

    # Receive.

    def receive(self, channels, block):

        if block:
            consumer_tags = {}
            for channel in channels:
                tag = self.amqp_channel.basic_consume(
                    partial(self.consume_message, consumer_tags),
                    queue=self.get_queue_name(channel),
                )
                consumer_tags[tag] = channel
        else:
            if not channels:
                # All channels gave 404 error.
                self.resolve.set_result((None, None))
                return
            channels = list(channels)  # Daphne sometimes pass dict.keys()
            channel = channels[0]
            no_message = partial(self.no_message, channels[1:])
            self.amqp_channel.add_callback(no_message, [Basic.GetEmpty])
            no_queue = partial(self.no_queue, channels[1:])
            self.amqp_channel.add_on_close_callback(no_queue)
            self.resolve.add_done_callback(
                lambda future: self.amqp_channel.callbacks.remove(
                    self.amqp_channel.channel_number,
                    '_on_channel_close',
                    no_queue,
                ),
            )
            self.amqp_channel.basic_get(
                partial(self.get_message, channel, no_message),
                queue=self.get_queue_name(channel),
            )

    # FIXME: If we tries to consume from list of channels.  One of
    # consumer queues doesn't exists.  Channel is closed with 404
    # error. We will never consume from existing queues and will
    # return empty response.
    #
    # FIXME: If we tries to consume from channel which queue doesn't
    # exist at this time.  We consume with blocking == True.  We must
    # start consuming at the moment when queue were declared.

    def consume_message(self, consumer_tags, amqp_channel, method_frame,
                        properties, body):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        for tag in consumer_tags:
            amqp_channel.basic_cancel(consumer_tag=tag)
        channel = consumer_tags[method_frame.consumer_tag]
        message = self.deserialize(body)
        self.resolve.set_result((channel, message))

    def get_message(self, channel, no_message, amqp_channel, method_frame,
                    properties, body):

        amqp_channel.callbacks.remove(
            amqp_channel.channel_number,
            Basic.GetEmpty,
            no_message,
        )
        amqp_channel.basic_ack(method_frame.delivery_tag)
        message = self.deserialize(body)
        self.resolve.set_result((channel, message))

    def no_message(self, channels, method_frame):

        if channels:
            self.receive(
                channels=channels,
                block=False,
            )
        else:
            self.resolve.set_result((None, None))

    def no_queue(self, channels, amqp_channel, code, msg):

        idents = {v: k for k, v in self.numbers.items()}
        ident = idents[amqp_channel.channel_number]
        del self.channels[amqp_channel.channel_number]
        del self.numbers[ident]
        del self.futures[amqp_channel]
        self.process(
            ident,
            partial(self.receive, channels, block=False),
            self.resolve,
        )

    # New channel.

    def new_channel(self):

        self.amqp_channel.queue_declare(
            lambda method_frame: self.resolve.set_result(
                method_frame.method.queue,
            ),
        )

    # Groups.

    def group_add(self, group, channel):

        # FIXME: Is it possible to do this things in parallel?
        self.expire_group_member(group, channel)

        def after_bind(method_frame):

            self.resolve.set_result(None)

        def bind_channel(method_frame):

            self.amqp_channel.queue_bind(
                after_bind,
                queue=self.get_queue_name(channel),
                exchange=channel,
            )

        def bind_group(method_frame):

            self.amqp_channel.exchange_bind(
                bind_channel,
                destination=channel,
                source=group,
            )

        def declare_channel(method_frame):
            if '!' in channel or '?' in channel:
                bind_group(None)
            else:
                self.amqp_channel.queue_declare(
                    bind_group,
                    queue=channel,
                    arguments={'x-dead-letter-exchange': self.dead_letters},
                )

        def declare_member(method_frame):
            self.amqp_channel.exchange_declare(
                declare_channel,
                exchange=channel,
                exchange_type='fanout',
            )

        self.amqp_channel.exchange_declare(
            declare_member,
            exchange=group,
            exchange_type='fanout',
        )

    def group_discard(self, group, channel):

        self.amqp_channel.exchange_unbind(
            lambda method_frame: self.resolve.set_result(None),
            destination=channel,
            source=group,
        )

    def send_group(self, group, message):

        body = self.serialize(message)
        self.amqp_channel.basic_publish(
            exchange=group,
            routing_key='',
            body=body,
            properties=self.publish_properties,
        )

    # Dead letters processing.

    def on_dead_letter_channel_open(self):

        self.amqp_channel.add_on_close_callback(
            self.on_dead_letter_channel_close,
        )
        self.declare_dead_letters()

    def on_dead_letter_channel_close(self, amqp_channel, code, msg):

        self.process(None, self.on_dead_letter_channel_open, Future())

    def expire_group_member(self, group, channel):

        ttl = self.group_expiry * 1000
        self.amqp_channel.queue_declare(
            partial(self.push_marker, group, channel),
            queue=self.get_expire_marker(group, channel),
            arguments={
                'x-dead-letter-exchange': self.dead_letters,
                'x-message-ttl': ttl,
                # FIXME: make this delay as little as possible.
                'x-expires': ttl + 500,
                'x-max-length': 1,
            },
        )

    def push_marker(self, group, channel, method_frame):

        body = self.serialize({
            'group': group,
            'channel': channel,
        })
        self.amqp_channel.basic_publish(
            exchange='',
            routing_key=self.get_expire_marker(group, channel),
            body=body,
        )

    def get_expire_marker(self, group, channel):

        return 'expire.bind.%s.%s' % (group, self.get_queue_name(channel))

    def declare_dead_letters(self):

        def consume(method_frame):

            self.amqp_channel.basic_consume(
                self.on_dead_letter,
                queue=self.dead_letters,
            )

        def do_bind(method_frame):

            self.amqp_channel.queue_bind(
                consume,
                queue=self.dead_letters,
                exchange=self.dead_letters,
            )

        def declare_queue(method_frame):

            self.amqp_channel.queue_declare(
                do_bind,
                queue=self.dead_letters,
            )

        self.amqp_channel.exchange_declare(
            declare_queue,
            exchange=self.dead_letters,
            exchange_type='fanout',
        )

    def on_dead_letter(self, amqp_channel, method_frame, properties, body):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        # FIXME: what the hell zero means here?
        queue = properties.headers['x-death'][0]['queue']
        reason = properties.headers['x-death'][0]['reason']
        if self.is_expire_marker(queue) and reason == 'expired':
            message = self.deserialize(body)
            group = message['group']
            channel = message['channel']
            self.group_discard(group, channel)
        elif not self.is_expire_marker(queue):
            amqp_channel.exchange_delete(exchange=queue)

    def is_expire_marker(self, queue):

        return queue.startswith('expire.bind.')

    # Serialization.

    def serialize(self, message):

        value = msgpack.packb(message, use_bin_type=True)
        return value

    def deserialize(self, message):

        return msgpack.unpackb(message, encoding='utf8')


class ConnectionThread(Thread):
    """
    Thread holding connection.

    Separate heartbeat frames processing from actual work.
    """

    def __init__(self, url, expiry, group_expiry, capacity, channel_capacity):

        super(ConnectionThread, self).__init__()
        self.daemon = True
        self.calls = queue.Queue()
        self.amqp = AMQP(url, expiry, group_expiry, capacity, channel_capacity,
                         self.calls)

    def run(self):

        self.amqp.run()

    def schedule(self, f, *args, **kwargs):

        future = Future()
        self.calls.put(
            (get_ident(), partial(f, *args, **kwargs), future),
        )
        return future


class RabbitmqChannelLayer(BaseChannelLayer):

    extensions = ['groups']

    def __init__(self,
                 url,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None):

        super(RabbitmqChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )
        self.thread = ConnectionThread(url, expiry, group_expiry, capacity,
                                       channel_capacity)
        self.thread.start()

    def send(self, channel, message):

        future = self.thread.schedule(self.thread.amqp.send, channel, message)
        return future.result()

    def receive(self, channels, block=False):

        future = self.thread.schedule(
            self.thread.amqp.receive,
            channels,
            block,
        )
        return future.result()

    def new_channel(self, pattern):

        assert pattern.endswith('!') or pattern.endswith('?')
        future = self.thread.schedule(self.thread.amqp.new_channel)
        queue_name = future.result()
        channel = pattern + queue_name
        return channel

    def declare_channel(self, channel):

        future = self.thread.schedule(
            self.thread.amqp.declare_channel,
            channel,
        )
        return future.result()

    def group_add(self, group, channel):

        future = self.thread.schedule(
            self.thread.amqp.group_add,
            group,
            channel,
        )
        return future.result()

    def group_discard(self, group, channel):

        future = self.thread.schedule(
            self.thread.amqp.group_discard,
            group,
            channel,
        )
        return future.result()

    def send_group(self, group, message):

        self.thread.schedule(self.thread.amqp.send_group, group, message)


# TODO: is it optimal to read bytes from content frame, call python
# decode method to convert it to string and than parse it with
# msgpack?  We should minimize useless work on message receive.


def worker_start_hook(sender, **kwargs):
    """Declare necessary queues we gonna listen."""

    layer_wrapper = sender.channel_layer
    layer = layer_wrapper.channel_layer
    if not isinstance(layer, RabbitmqChannelLayer):
        return
    channels = sender.apply_channel_filters(layer_wrapper.router.channels)
    for channel in channels:
        layer.declare_channel(channel)


# FIXME: This must be optional since we don't require channels package
# to be installed.
worker_ready.connect(worker_start_hook)
