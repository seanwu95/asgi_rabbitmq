import random
import string
from functools import partial

import msgpack
from asgiref.base_layer import BaseChannelLayer
from pika import BlockingConnection, URLParameters
from pika.exceptions import ChannelClosed
from pika.spec import BasicProperties


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
        parameters = URLParameters(url)
        self.amqp_connection = BlockingConnection(parameters)
        self.amqp_channel = self.amqp_connection.channel()

    def send(self, channel, message):

        if not self.amqp_channel.is_open:  # FIXME: duplication :(
            self.amqp_channel = self.amqp_connection.channel()
        reply = self.amqp_channel.queue_declare(queue=channel)
        if reply.method.message_count >= self.capacity:
            raise self.ChannelFull
        body = self.serialize(message)
        expiration = str(self.expiry * 1000)
        properties = BasicProperties(expiration=expiration)
        self.amqp_channel.publish(
            exchange='',
            routing_key=channel,
            body=body,
            properties=properties,
        )

    def receive(self, channels, block=False):

        if not self.amqp_channel.is_open:  # FIXME: duplication :(
            self.amqp_channel = self.amqp_connection.channel()
        result = Result()
        callbacks = {
            channel_name: partial(on_message, self, result, channel_name)
            for channel_name in channels
        }

        # FIXME: don't consume on queues from previous `receive`
        # call.  Clean up `amqp_channel` state.
        #
        # FIXME: block if all queues doesn't exists
        consumed = False
        while not consumed:
            for channel_name, callback in callbacks.items():
                try:
                    self.amqp_channel.basic_consume(
                        callback, queue=channel_name)
                except ChannelClosed as e:
                    if not_found_error(e):
                        del callbacks[channel_name]
                        # FIXME: duplication :(
                        self.amqp_channel = self.amqp_connection.channel()
                        break
                    else:
                        raise
            else:
                consumed = True

        time_limit = None if block else 0
        self.amqp_connection.process_data_events(time_limit)

        if not block and not result:
            return None, None

        channel, method_frame, properties, body = result.value
        message = self.deserialize(body)
        return channel, message

    def new_channel(self, pattern):

        assert pattern.endswith('!') or pattern.endswith('?')

        while True:
            chars = (random.choice(string.ascii_letters) for _ in range(12))
            random_string = ''.join(chars)
            channel = pattern + random_string
            try:
                self.amqp_channel.queue_declare(queue=channel, passive=True)
            except ChannelClosed as e:
                # FIXME: Channel is always closed on next retry.  Open
                # new one here.
                if not_found_error(e):
                    return channel
                else:
                    raise

    def group_add(self, group, channel):

        self.amqp_channel.exchange_declare(
            exchange=group,
            exchange_type='fanout',
        )
        self.amqp_channel.queue_declare(queue=channel)
        self.amqp_channel.queue_bind(queue=channel, exchange=group)

    def group_discard(self, group, channel):

        self.amqp_channel.queue_unbind(queue=channel, exchange=group)

    def send_group(self, group, message):

        # FIXME: What about expiration property here?
        body = self.serialize(message)
        self.amqp_channel.publish(
            exchange=group,
            routing_key='',
            body=body,
        )

    def serialize(self, message):

        value = msgpack.packb(message, use_bin_type=True)
        return value

    def deserialize(self, message):

        return msgpack.unpackb(message, encoding="utf8")


def on_message(layer, result, channel_name, channel, method_frame, properties,
               body):
    layer.amqp_channel.basic_ack(method_frame.delivery_tag)
    layer.amqp_channel.stop_consuming()
    result.value = (channel_name, method_frame, properties, body)


class Result(object):

    def __init__(self):

        self.value = None

    def __bool__(self):

        return self.value is not None

    __nonzero__ = __bool__


def not_found_error(exception):

    return exception.args[0] == 404


# TODO: is it optimal to read bytes from content frame, call python
# decode method to convert it to string and than parse it with
# msgpack?  We should minimize useless work on message receive.
