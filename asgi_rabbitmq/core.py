import os
from functools import partial, wraps

import msgpack
from asgiref.base_layer import BaseChannelLayer
from channels.signals import worker_ready
from pika import SelectConnection, URLParameters
from pika.spec import BasicProperties

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

if 'BENCHMARK' in os.environ:
    from .amqp import bench
else:

    def bench(f):
        return f


class PropagatedError(Exception):
    """Exception raised to show error from the connection thread."""


class AMQP(object):

    dead_letters = 'dead-letters'
    Parameters = URLParameters
    Connection = SelectConnection

    # Poor man's dependency injection.
    if 'BENCHMARK' in os.environ:
        from .amqp import DebugConnection as Connection

    def __init__(self, url, expiry, group_expiry, capacity, channel_capacity,
                 method_calls):

        self.parameters = self.Parameters(url)
        self.connection = self.Connection(
            parameters=self.parameters,
            on_open_callback=self.on_connection_open,
        )
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.capacity = capacity
        self.channel_capacity = channel_capacity
        self.method_calls = method_calls
        self.channels = {}

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
            ident, method = self.method_calls.get_nowait()
            self.process(ident, method)
        except queue.Empty:
            pass
        self.connection.add_timeout(0.01, self.check_method_call)

    def process(self, ident, method):

        if ident in self.channels:
            amqp_channel = self.channels[ident]
            if amqp_channel.is_open:
                method(amqp_channel=amqp_channel)
                return
        self.connection.channel(
            partial(
                self.register_channel,
                ident=ident,
                method=method,
            ))

    def register_channel(self, amqp_channel, ident, method=None):

        self.channels[ident] = amqp_channel
        if method:
            method(amqp_channel=amqp_channel)

    # Utilities.

    def get_queue_name(self, channel):

        if '!' in channel:
            return channel.rsplit('!', 1)[-1]
        elif '?' in channel:
            return channel.rsplit('?', 1)[-1]
        else:
            return channel

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

            def onerror(channel, code, msg):
                resolve.set_exception(PropagatedError(code, msg))

            resolve.add_done_callback(
                partial(
                    amqp_channel.callbacks.remove,
                    amqp_channel.channel_number,
                    '_on_channel_close',
                    onerror,
                ))
            amqp_channel.add_on_close_callback(onerror)
            method(self, **kwargs)

        return method_wrapper

    # Send.

    @propagate_on_close
    def send(self, amqp_channel, channel, message, resolve):

        # FIXME: Avoid constant queue declaration.  Or at least try to
        # minimize its impact to system.
        queue = self.get_queue_name(channel)
        publish_message = partial(
            self.publish_message,
            amqp_channel=amqp_channel,
            channel=queue,
            message=message,
            resolve=resolve,
        )
        self.declare_channel(
            amqp_channel=amqp_channel,
            channel=queue,
            passive=True if '!' in channel or '?' in channel else False,
            callback=publish_message,
        )

    def declare_channel(self, amqp_channel, channel, passive, callback):

        amqp_channel.queue_declare(
            lambda method_frame: callback(method_frame=method_frame),
            queue=channel,
            passive=passive,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

    @propagate_error
    def publish_message(self, amqp_channel, channel, message, resolve,
                        method_frame):

        if method_frame.method.message_count >= self.capacity:
            raise RabbitmqChannelLayer.ChannelFull
        body = self.serialize(message)
        expiration = str(self.expiry * 1000)
        properties = BasicProperties(expiration=expiration)
        amqp_channel.basic_publish(
            exchange='',
            routing_key=channel,
            body=body,
            properties=properties,
        )
        resolve.set_result(None)

    # Receive.

    @propagate_error
    @propagate_on_close
    def receive(self, amqp_channel, channels, block, resolve):

        consumer_tags = {}
        timeout = partial(
            self.consume_timeout,
            amqp_channel=amqp_channel,
            consumer_tags=consumer_tags,
            resolve=resolve,
        )
        # FIXME: Set as less as possible, should be configurable.
        # FIXME: Support `block is True` variant.
        timeout_id = amqp_channel.connection.add_timeout(0.1, timeout)
        callback = partial(
            self.consume_message,
            timeout_id=timeout_id,
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

    @propagate_error
    def consume_timeout(self, amqp_channel, consumer_tags, resolve):
        # If channel is closed here, it means we tried to consume from
        # non existing queue.
        if amqp_channel.is_open:
            for tag in consumer_tags:
                amqp_channel.basic_cancel(consumer_tag=tag)
        resolve.set_result((None, None))

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
                        timeout_id, consumer_tags, resolve):
        amqp_channel.connection.remove_timeout(timeout_id)
        amqp_channel.basic_ack(method_frame.delivery_tag)
        for tag in consumer_tags:
            amqp_channel.basic_cancel(consumer_tag=tag)
        channel = consumer_tags[method_frame.consumer_tag]
        message = self.deserialize(body)
        resolve.set_result((channel, message))

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

    # FIXME: Handle queue.Full exception in all method calls blow.

    @bench
    def send(self, channel, message):

        future = Future()
        self.schedule(
            partial(
                self.thread.amqp.send,
                channel=channel,
                message=message,
                resolve=future,
            ))
        return future.result()

    @bench
    def receive(self, channels, block=False):

        future = Future()
        self.schedule(
            partial(
                self.thread.amqp.receive,
                channels=channels,
                block=block,
                resolve=future,
            ))
        try:
            return future.result()
        except PropagatedError:
            return None, None

    @bench
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

    @bench
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

    @bench
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

    @bench
    def send_group(self, group, message):

        self.schedule(
            partial(
                self.thread.amqp.send_group,
                group=group,
                message=message,
            ))

    def schedule(self, f):

        self.thread.calls.put((get_ident(), f))


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
