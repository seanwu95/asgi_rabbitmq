from .core import RabbitmqChannelLayer


class RabbitmqLocalChannelLayer(RabbitmqChannelLayer):

    def __init__(self,
                 url,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None,
                 symmetric_encryption_keys=None,
                 prefix='asgi'):

        super(RabbitmqLocalChannelLayer, self).__init__(
            url,
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
            symmetric_encryption_keys=symmetric_encryption_keys,
        )
        try:
            from asgi_ipc import IPCChannelLayer
        except ImportError:
            raise ValueError(
                'You must install asgi_ipc to use the local variant',
            )
        self.local_layer = IPCChannelLayer(
            prefix,
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )

    def send(self, channel, message):

        if "!" in channel or "?" in channel:
            return super(RabbitmqLocalChannelLayer, self).send(
                channel,
                message,
            )
        else:
            return self.local_layer.send(channel, message)

    def receive(self, channels, block=False):

        num_remote = len([ch for ch in channels if "!" in ch or "?" in ch])
        num_local = len(channels) - num_remote
        if num_local and num_remote:
            result = self.local_layer.receive(channels, block=False)
            if result[0] is not None:
                return result
            return super(RabbitmqLocalChannelLayer, self).receive(
                channels,
                block,
            )
        elif num_local:
            return self.local_layer.receive(channels, block)
        else:
            return super(RabbitmqLocalChannelLayer, self).receive(
                channels,
                block,
            )
