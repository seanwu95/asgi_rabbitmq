import random
import string
import threading
from collections import defaultdict
from functools import partial

import msgpack
from asgiref.base_layer import BaseChannelLayer
from channels.signals import worker_ready
from pika import SelectConnection, URLParameters
from pika.spec import BasicProperties

try:
    import queue
except ImportError:
    import Queue as queue


class AMQP(object):

    dead_letters = 'dead-letters'

    def __init__(self, url, expiry, group_expiry, capacity, channel_capacity,
                 method_calls):

        self.parameters = URLParameters(url)
        self.connection = SelectConnection(
            parameters=self.parameters,
            on_open_callback=self.on_connection_open,
        )
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.capacity = capacity
        self.channel_capacity = channel_capacity
        self.method_calls = method_calls

    def run(self):

        self.connection.ioloop.start()

    def on_connection_open(self, connection):

        connection.channel(self.on_channel_open)

    def on_channel_open(self, amqp_channel):

        self.declare_dead_letters(amqp_channel)
        self.check_method_call(amqp_channel)

    def check_method_call(self, amqp_channel):

        try:
            method = self.method_calls.get_nowait()
            method(amqp_channel=amqp_channel)
        except queue.Empty:
            pass
        amqp_channel.connection.add_timeout(
            0.01,
            partial(self.check_method_call, amqp_channel),
        )

    def send(self, amqp_channel, channel, message, result):

        # FIXME: Avoid constant queue declaration.  Or at least try to
        # minimize its impact to system.
        declare_queue = partial(
            amqp_channel.queue_declare,
            queue=channel,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

        # FIXME: remove nested function definition.
        def callback(method_frame):
            if method_frame.method.message_count >= self.capacity:
                result.put(RabbitmqChannelLayer.raise_channel_full)
                return
            body = self.serialize(message)
            expiration = str(self.expiry * 1000)
            properties = BasicProperties(expiration=expiration)
            amqp_channel.basic_publish(
                exchange='',
                routing_key=channel,
                body=body,
                properties=properties,
            )
            result.put(lambda: None)

        declare_queue(callback)

    def receive(self, amqp_channel, channels, block, result):

        consumer_tags = {}

        def timeout_callback():
            for tag in consumer_tags:
                amqp_channel.basic_cancel(consumer_tag=tag)
            result.put(lambda: (None, None))

        # FIXME: Set as less as possible, should be configurable.
        # FIXME: Support `block is True` variant.
        timeout_id = amqp_channel.connection.add_timeout(0.1, timeout_callback)

        # FIXME: Don't define function each time.
        def callback(amqp_channel, method_frame, properties, body):
            amqp_channel.connection.remove_timeout(timeout_id)
            amqp_channel.basic_ack(method_frame.delivery_tag)
            for tag in consumer_tags:
                amqp_channel.basic_cancel(consumer_tag=tag)
            channel = consumer_tags[method_frame.consumer_tag]
            message = self.deserialize(body)
            result.put(lambda: (channel, message))

        for channel in channels:
            tag = amqp_channel.basic_consume(callback, queue=channel)
            consumer_tags[tag] = channel

    def group_add(self, amqp_channel, group, channel, result):

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
            queue=channel,
            exchange=channel,
        )
        declare_group(
            lambda method_frame: declare_member(
                lambda method_frame: declare_channel(
                    lambda method_frame: bind_group(
                        lambda method_frame: bind_channel(
                            lambda method_frame: result.put(lambda: None),
                        ),
                    ),
                ),
            ),
        )

    def group_discard(self, amqp_channel, group, channel, result):

        unbind_member = partial(
            amqp_channel.exchange_unbind,
            destination=channel,
            source=group,
        )
        unbind_member(lambda method_frame: result.put(lambda: None))

    def send_group(self, amqp_channel, group, message):

        # FIXME: What about expiration property here?
        body = self.serialize(message)
        amqp_channel.basic_publish(
            exchange=group,
            routing_key='',
            body=body,
        )

    def expire_group_member(self, amqp_channel, group, channel):

        expire_marker = 'expire.bind.%s.%s' % (group, channel)
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
            self.group_discard(amqp_channel, group, channel, skip_result)
        else:
            amqp_channel.exchange_delete(exchange=queue)

    def serialize(self, message):

        value = msgpack.packb(message, use_bin_type=True)
        return value

    def deserialize(self, message):

        return msgpack.unpackb(message, encoding='utf8')

    def is_expire_marker(self, queue):

        return queue.startswith('expire.bind.')


class ConnectionThread(threading.Thread):
    """
    Thread holding connection.

    Separate heartbeat frames processing from actual work.
    """

    def __init__(self, url, expiry, group_expiry, capacity, channel_capacity):

        super(ConnectionThread, self).__init__()
        self.daemon = True
        self.calls = queue.Queue()
        self.results = defaultdict(partial(queue.Queue, maxsize=1))
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

    def send(self, channel, message):

        self.thread.calls.put(
            partial(
                self.thread.amqp.send,
                channel=channel,
                message=message,
                result=self.result_queue,
            ))
        return self.result

    def receive(self, channels, block=False):

        self.thread.calls.put(
            partial(
                self.thread.amqp.receive,
                channels=channels,
                block=block,
                result=self.result_queue,
            ))
        return self.result

    def new_channel(self, pattern):

        assert pattern.endswith('!') or pattern.endswith('?')

        while True:
            chars = (random.choice(string.ascii_letters) for _ in range(12))
            random_string = ''.join(chars)
            channel = pattern + random_string
            # FIXME: We should return new channel only after 404 reply
            # from queue_declare method.
            return channel

    def group_add(self, group, channel):

        self.thread.calls.put(
            partial(
                self.thread.amqp.group_add,
                group=group,
                channel=channel,
                result=self.result_queue,
            ))
        return self.result

    def group_discard(self, group, channel):

        self.thread.calls.put(
            partial(
                self.thread.amqp.group_discard,
                group=group,
                channel=channel,
                result=self.result_queue,
            ))
        return self.result

    def send_group(self, group, message):

        self.thread.calls.put(
            partial(
                self.thread.amqp.send_group,
                group=group,
                message=message,
            ))

    @property
    def result_queue(self):

        return self.thread.results[threading.get_ident()]

    @property
    def result(self):

        result_getter = self.result_queue.get()
        result = result_getter()
        return result

    @classmethod
    def raise_channel_full(cls):

        raise cls.ChannelFull


class NullQueue(object):
    """`queue.Queue` stub."""

    def get(self, block=True, timeout=None):
        pass

    def get_nowait(self):
        pass

    def put(self, item, block=True, timeout=None):
        pass

    def put_nowait(self, item):
        pass


skip_result = NullQueue()

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
        # FIXME: duplication, encapsulation violation.
        layer.amqp_channel.queue_declare(
            queue=channel,
            arguments={'x-dead-letter-exchange': layer.dead_letters},
        )


# FIXME: This must be optional since we don't require channels package
# to be installed.
worker_ready.connect(worker_start_hook)
