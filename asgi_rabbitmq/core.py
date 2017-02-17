import weakref
from functools import partial, wraps

import msgpack
from asgiref.base_layer import BaseChannelLayer
from channels.signals import worker_ready
from pika import SelectConnection, URLParameters
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


class PropagatedError(Exception):
    """Exception raised to show error from the connection thread."""


class LayerSelectConnection(SelectConnection):

    def __init__(self, *args, **kwargs):

        self.amqp_ref = kwargs.pop('amqp_ref')
        super(LayerSelectConnection, self).__init__(*args, **kwargs)

    def _process_callbacks(self, frame_value):

        amqp = self.amqp_ref()
        try:
            amqp.amqp_channel = amqp.channels.get(frame_value.channel_number)
            amqp.resolve = amqp.futures.get(amqp.amqp_channel)
            return super(LayerSelectConnection,
                         self)._process_callbacks(frame_value)
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

        connection.channel(self.on_channel_open)
        self.check_method_call()

    def on_channel_open(self, amqp_channel):

        amqp_channel.add_on_close_callback(self.on_channel_close)
        self.declare_dead_letters(amqp_channel)

    def on_channel_close(self, amqp_channel, code, msg):

        # FIXME: Check if error is recoverable.
        amqp_channel.connection.channel(self.on_channel_open)

    def check_method_call(self):

        try:
            ident, method, future = self.method_calls.get_nowait()
            self.process(ident, method, future)
        except queue.Empty:
            pass
        self.connection.add_timeout(0.01, self.check_method_call)

    def process(self, ident, method, future):

        # FIXME: Remove after all layer methods will be ported to the
        # new schedule mechanism.
        if future is None:
            if ident in self.numbers:
                amqp_channel = self.channels[self.numbers[ident]]
                if amqp_channel.is_open:
                    method(amqp_channel=amqp_channel)
                    return

            def _old_register_channel(amqp_channel):
                self.numbers[ident] = amqp_channel.channel_number
                self.channels[amqp_channel.channel_number] = amqp_channel
                method(amqp_channel=amqp_channel)

            self.connection.channel(_old_register_channel)
            return
        # -----
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

    # FIXME: remove this two decorators when transition to the new
    # schedule mechanism will be finished.

    def propagate_error(method):

        @wraps(method)
        def method_wrapper(self, **kwargs):

            try:
                method(self, **kwargs)
            except Exception as error:
                kwargs['resolve'].set_exception(error)

        return method_wrapper

    def propagate_on_close(method):

        @wraps(method)
        def method_wrapper(self, **kwargs):

            amqp_channel = kwargs['amqp_channel']
            resolve = kwargs['resolve']
            custom_handler = method(self, **kwargs)

            def onerror(channel, code, msg):
                if custom_handler:
                    custom_handler(channel, code, msg)
                else:
                    resolve.set_exception(PropagatedError(code, msg))

            resolve.add_done_callback(
                partial(
                    amqp_channel.callbacks.remove,
                    amqp_channel.channel_number,
                    '_on_channel_close',
                    onerror,
                ))
            amqp_channel.add_on_close_callback(onerror)

        return method_wrapper

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
        expiration = str(self.expiry * 1000)
        properties = BasicProperties(expiration=expiration)
        self.amqp_channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=body,
            properties=properties,
        )
        self.resolve.set_result(None)

    # Declare channel.

    def declare_channel(self, amqp_channel, channel, passive, callback):

        amqp_channel.queue_declare(
            lambda method_frame: callback(method_frame=method_frame),
            queue=channel,
            passive=passive,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

    # Receive.

    @propagate_error
    @propagate_on_close
    def receive(self, amqp_channel, channels, block, resolve):

        if block:
            consumer_tags = {}
            callback = partial(
                self.consume_message,
                consumer_tags=consumer_tags,
                resolve=resolve,
            )
            for channel in channels:
                tag = amqp_channel.basic_consume(
                    lambda amqp_channel,
                    method_frame,
                    properties,
                    body: callback(
                        amqp_channel=amqp_channel,
                        method_frame=method_frame,
                        properties=properties,
                        body=body,
                    ),
                    queue=self.get_queue_name(channel),
                )
                consumer_tags[tag] = channel
        else:
            if not channels:
                # All channels gave 404 error.
                resolve.set_result((None, None))
                return
            channels = list(channels)  # Daphne sometimes pass dict.keys()
            channel = channels[0]
            get_empty_callback = lambda method_frame: self.no_message(
                method_frame=method_frame,
                amqp_channel=amqp_channel,
                channels=channels[1:],
                resolve=resolve)
            amqp_channel.add_callback(
                replies=[Basic.GetEmpty],
                callback=get_empty_callback,
            )
            amqp_channel.basic_get(
                queue=self.get_queue_name(channel),
                callback=lambda amqp_channel, method_frame, properties, body: (
                    self.get_message(
                        amqp_channel=amqp_channel,
                        method_frame=method_frame,
                        properties=properties,
                        body=body,
                        channel=channel,
                        resolve=resolve,
                        get_empty_callback=get_empty_callback)))
            return lambda amqp_channel, code, msg: self.no_queue(
                amqp_channel=amqp_channel,
                code=code,
                msg=msg,
                channels=channels[1:],
                resolve=resolve,
            )

    # FIXME: If we tries to consume from list of channels.  One of
    # consumer queues doesn't exists.  Channel is closed with 404
    # error. We will never consume from existing queues and will
    # return empty response.
    #
    # FIXME: If we tries to consume from channel which queue doesn't
    # exist at this time.  We consume with blocking == True.  We must
    # start consuming at the moment when queue were declared.

    @propagate_error
    def consume_message(self, amqp_channel, method_frame, properties, body,
                        consumer_tags, resolve):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        for tag in consumer_tags:
            amqp_channel.basic_cancel(consumer_tag=tag)
        channel = consumer_tags[method_frame.consumer_tag]
        message = self.deserialize(body)
        resolve.set_result((channel, message))

    @propagate_error
    def get_message(self, amqp_channel, method_frame, properties, body,
                    channel, resolve, get_empty_callback):

        amqp_channel.callbacks.remove(
            amqp_channel.channel_number,
            Basic.GetEmpty,
            get_empty_callback,
        )
        amqp_channel.basic_ack(method_frame.delivery_tag)
        message = self.deserialize(body)
        resolve.set_result((channel, message))

    @propagate_error
    def no_message(self, amqp_channel, method_frame, channels, resolve):

        if channels:
            self.receive(
                amqp_channel=amqp_channel,
                channels=channels,
                block=False,
                resolve=resolve,
            )
        else:
            resolve.set_result((None, None))

    @propagate_error
    def no_queue(self, amqp_channel, code, msg, channels, resolve):

        self.method_calls.put((resolve.ident, partial(
            self.receive,
            channels=channels,
            block=False,
            resolve=resolve,
        ), None))

    # New channel.

    @propagate_error
    @propagate_on_close
    def new_channel(self, amqp_channel, resolve):

        amqp_channel.queue_declare(
            lambda method_frame: resolve.set_result(method_frame.method.queue),
        )

    # Groups.

    @propagate_error
    @propagate_on_close
    def group_add(self, amqp_channel, group, channel, resolve):

        # FIXME: Is it possible to do this things in parallel?
        self.expire_group_member(amqp_channel, group, channel)
        declare_group = partial(
            amqp_channel.exchange_declare,
            exchange=group,
            exchange_type='fanout',
        )
        declare_member = partial(
            amqp_channel.exchange_declare,
            exchange=channel,
            exchange_type='fanout',
        )
        if '!' in channel or '?' in channel:
            declare_channel = lambda callback: callback(method_frame=None)
        else:
            declare_channel = partial(
                amqp_channel.queue_declare,
                queue=channel,
                arguments={'x-dead-letter-exchange': self.dead_letters},
            )
        bind_group = partial(
            amqp_channel.exchange_bind,
            destination=channel,
            source=group,
        )
        bind_channel = partial(
            amqp_channel.queue_bind,
            queue=self.get_queue_name(channel),
            exchange=channel,
        )
        declare_group(
            lambda method_frame: declare_member(
                lambda method_frame: declare_channel(
                    lambda method_frame: bind_group(
                        lambda method_frame: bind_channel(
                            lambda method_frame: resolve.set_result(None),
                        ),
                    ),
                ),
            ),
        )

    @propagate_error
    @propagate_on_close
    def group_discard(self, amqp_channel, group, channel, resolve):

        amqp_channel.exchange_unbind(
            lambda method_frame: resolve.set_result(None),
            destination=channel,
            source=group,
        )

    def send_group(self, amqp_channel, group, message):

        # FIXME: What about expiration property here?
        body = self.serialize(message)
        amqp_channel.basic_publish(
            exchange=group,
            routing_key='',
            body=body,
        )

    # Dead letters processing.

    def expire_group_member(self, amqp_channel, group, channel):

        expire_marker = 'expire.bind.%s.%s' % (group,
                                               self.get_queue_name(channel))
        ttl = self.group_expiry * 1000

        declare_marker = partial(
            amqp_channel.queue_declare,
            queue=expire_marker,
            arguments={
                'x-dead-letter-exchange': self.dead_letters,
                'x-message-ttl': ttl,
                # FIXME: make this delay as little as possible.
                'x-expires': ttl + 500,
                'x-max-length': 1,
            })

        def push_marker(method_frame):
            body = self.serialize({
                'group': group,
                'channel': channel,
            })
            amqp_channel.basic_publish(
                exchange='',
                routing_key=expire_marker,
                body=body,
            )

        declare_marker(push_marker)

    def declare_dead_letters(self, amqp_channel):

        declare_exchange = partial(
            amqp_channel.exchange_declare,
            exchange=self.dead_letters,
            exchange_type='fanout',
        )
        declare_queue = partial(
            amqp_channel.queue_declare,
            queue=self.dead_letters,
        )
        do_bind = partial(
            amqp_channel.queue_bind,
            queue=self.dead_letters,
            exchange=self.dead_letters,
        )
        consume = partial(
            self.consume_from_dead_letters,
            amqp_channel=amqp_channel,
        )
        declare_exchange(
            lambda method_frame: declare_queue(
                lambda method_frame: do_bind(
                    lambda method_frame: consume(),
                ),
            ),
        )

    def consume_from_dead_letters(self, amqp_channel):

        amqp_channel.basic_consume(
            self.on_dead_letter,
            queue=self.dead_letters,
        )

    def on_dead_letter(self, amqp_channel, method_frame, properties, body):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        # FIXME: Ignore max-length dead-letters.
        # FIXME: what the hell zero means here?
        queue = properties.headers['x-death'][0]['queue']
        if self.is_expire_marker(queue):
            message = self.deserialize(body)
            group = message['group']
            channel = message['channel']
            self.group_discard(
                amqp_channel=amqp_channel,
                group=group,
                channel=channel,
                resolve=Future(),
            )
        else:
            amqp_channel.exchange_delete(exchange=queue)

    def is_expire_marker(self, queue):

        return queue.startswith('expire.bind.')

    # Serialization.

    def serialize(self, message):

        value = msgpack.packb(message, use_bin_type=True)
        return value

    def deserialize(self, message):

        return msgpack.unpackb(message, encoding='utf8')

    del propagate_error
    del propagate_on_close


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

        future = Future()
        future.ident = get_ident()
        self.schedule(
            partial(
                self.thread.amqp.receive,
                channels=channels,
                block=block,
                resolve=future,
            ))
        return future.result()

    def new_channel(self, pattern):

        assert pattern.endswith('!') or pattern.endswith('?')
        future = Future()
        self.schedule(partial(
            self.thread.amqp.new_channel,
            resolve=future,
        ))
        queue_name = future.result()
        channel = pattern + queue_name
        return channel

    def declare_channel(self, channel):

        future = Future()
        self.schedule(
            partial(
                self.thread.amqp.declare_channel,
                channel=channel,
                passive=False,
                callback=lambda method_frame: future.set_result(None),
            ))
        return future.result()

    def group_add(self, group, channel):

        future = Future()
        self.schedule(
            partial(
                self.thread.amqp.group_add,
                group=group,
                channel=channel,
                resolve=future,
            ))
        return future.result()

    def group_discard(self, group, channel):

        future = Future()
        self.schedule(
            partial(
                self.thread.amqp.group_discard,
                group=group,
                channel=channel,
                resolve=future,
            ))
        return future.result()

    def send_group(self, group, message):

        self.schedule(
            partial(
                self.thread.amqp.send_group,
                group=group,
                message=message,
            ))

    def schedule(self, f):

        self.thread.calls.put((get_ident(), f, None))


# TODO: is it optimal to read bytes from content frame, call python
# decode method to convert it to string and than parse it with
# msgpack?  We should minimize useless work on message receive.
#
# FIXME: `retry_if_closed` and `propagate_error` not works with nested
# callbacks.


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
