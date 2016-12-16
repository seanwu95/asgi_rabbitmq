from asgiref.base_layer import BaseChannelLayer
from pika import BlockingConnection, URLParameters
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
        self.connection = BlockingConnection(parameters)

    def send(self, channel, message):

        amqp_channel = self.connection.channel()
        properties = BasicProperties(headers=message)
        amqp_channel.queue_declare(channel)
        amqp_channel.publish('', channel, '', properties)

    def receive(self, channels, block=False):

        amqp_channel = self.connection.channel()
        result = []

        def on_message(channel, method_frame, properties, body):

            amqp_channel.stop_consuming()
            result.append((channel, method_frame, properties, body))

        for channel in channels:
            amqp_channel.basic_consume(on_message, channel)

        amqp_channel.start_consuming()
        _, method_frame, properties, body = result.pop()
        channel = method_frame.routing_key
        message = properties.headers
        return channel, message

    def new_channel(self, pattern):

        pass

    def group_add(self, group, channel):

        pass

    def group_discard(self, group, channel):

        pass

    def group_channels(self, group):

        return []

    def send_group(self, group, message):

        pass
