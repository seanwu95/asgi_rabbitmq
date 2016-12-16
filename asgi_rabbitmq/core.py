from asgiref.base_layer import BaseChannelLayer
from pika import BlockingConnection, URLParameters


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

        pass

    def receive(self, channels, block=False):

        return None, None

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
