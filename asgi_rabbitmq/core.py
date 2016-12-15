from asgiref.base_layer import BaseChannelLayer


class RabbitmqChannelLayer(BaseChannelLayer):

    extensions = ['groups', 'twisted']

    def __init__(self,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None,
                 **kwargs):

        super(RabbitmqChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )
