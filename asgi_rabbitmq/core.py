from collections import deque
from functools import partial, wraps

import msgpack
from asgiref.base_layer import BaseChannelLayer
from channels.signals import worker_ready
from pika import SelectConnection, URLParameters
from pika.spec import BasicProperties
from twisted.internet import defer, reactor

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


class AMQP(object):

    dead_letters = 'dead-letters'
    Parameters = URLParameters
    Connection = SelectConnection

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
        self.messages = {}
        self.blocked = {}

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

    @propagate_on_close
    def send(self, amqp_channel, channel, message, resolve):

        routing_key = self.get_queue_name(channel)
        body = self.serialize(message)
        expiration = str(self.expiry * 1000)
        properties = BasicProperties(expiration=expiration)
        amqp_channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=body,
            properties=properties,
        )
        resolve.set_result(None)

    def declare_channel(self, amqp_channel, channel, resolve):

        amqp_channel.queue_declare(
            lambda method_frame: self.subscribe_to_channel(
                amqp_channel,
                channel,
                resolve,
            ),
            queue=channel,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

    def subscribe_to_channel(self, amqp_channel, channel, resolve):

        self.messages[channel] = deque()
        amqp_channel.basic_consume(
            partial(self.accept_message, channel=channel),
            queue=self.get_queue_name(channel),
            no_ack=True,
        )
        resolve.set_result(channel)

    def accept_message(self, amqp_channel, method_frame, properties, body,
                       channel):

        for resolve, channels in self.blocked.items():
            # FIXME: None channels is a hack here to try Twisted support.
            if channels is None or channel in channels:
                del self.blocked[resolve]
                resolve.set_result((channel, self.deserialize(body)))
                break
        else:
            self.messages[channel].append(body)

    # Receive.

    @propagate_error
    @propagate_on_close
    def receive(self, amqp_channel, channels, block, resolve):

        for channel in channels:
            messages = self.messages.get(channel)
            if messages:
                body = messages.popleft()
                message = self.deserialize(body)
                resolve.set_result((channel, message))
                break
        else:
            if block:
                self.blocked[resolve] = channels
            else:
                resolve.set_result((None, None))

    def receive_twisted(self, amqp_channel, resolve):

        for channel, messages in self.messages.items():
            if messages:
                body = messages.popleft()
                message = self.deserialize(body)
                resolve.set_result((channel, message))
                break
        else:
            self.blocked[resolve] = None

    # New channel.

    @propagate_error
    @propagate_on_close
    def new_channel(self, amqp_channel, pattern, resolve):

        amqp_channel.queue_declare(
            lambda method_frame: self.subscribe_to_channel(
                amqp_channel,
                pattern + method_frame.method.queue,
                resolve,
            ),
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
            no_ack=True,
        )

    def on_dead_letter(self, amqp_channel, method_frame, properties, body):

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

    extensions = ['groups', 'twisted']

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

    def receive(self, channels, block=False):

        future = Future()
        self.schedule(
            partial(
                self.thread.amqp.receive,
                channels=channels,
                block=block,
                resolve=future,
            ))
        return future.result()

    @defer.inlineCallbacks
    def receive_twisted(self):
        """Twisted-native implementation of receive."""

        deferred = defer.Deferred()

        def resolve_deferred(future):

            reactor.callFromThread(deferred.callback, future.result())

        future = Future()
        future.add_done_callback(resolve_deferred)
        self.schedule(
            partial(
                self.thread.amqp.receive_twisted,
                resolve=future,
            ))
        defer.returnValue((yield deferred))

    def new_channel(self, pattern):

        assert pattern.endswith('!') or pattern.endswith('?')
        future = Future()
        self.schedule(
            partial(
                self.thread.amqp.new_channel,
                pattern=pattern,
                resolve=future,
            ))
        return future.result()

    def declare_channel(self, channel):

        future = Future()
        self.schedule(
            partial(
                self.thread.amqp.declare_channel,
                channel=channel,
                resolve=future,
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
