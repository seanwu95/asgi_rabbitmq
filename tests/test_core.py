import pytest
from asgi_rabbitmq import RabbitmqChannelLayer
from asgiref.conformance import ConformanceTestCase


class RabbitmqChannelLayerTest(ConformanceTestCase):

    @pytest.fixture(autouse=True)
    def init_conformance_test(self, vhost):

        self.channel_layer = RabbitmqChannelLayer(vhost)

    expiry_delay = 1.1
    capacity_limit = 5
