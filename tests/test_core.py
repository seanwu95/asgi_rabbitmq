import pytest
from asgi_rabbitmq import RabbitmqChannelLayer
from asgiref.conformance import ConformanceTestCase


class RabbitmqChannelLayerTest(ConformanceTestCase):

    @pytest.fixture(autouse=True)
    def init_conformance_test(self, vhost):

        self.channel_layer = RabbitmqChannelLayer(
            vhost, expiry=1, group_expiry=2, capacity=5)

    expiry_delay = 1.1
    capacity_limit = 5

    def test_send_to_empty_group(self):
        """Send to empty group works as usual."""

        self.skip_if_no_extension('groups')
        self.channel_layer.send_group('tgroup_1', {'value': 'orange'})

    def test_discard_from_empty_group(self):
        """Discard from empty group works as usual."""

        self.skip_if_no_extension('groups')
        self.channel_layer.group_discard('tgroup_2', 'tg_test3')

    def test_group_channels(self):

        # TODO: figure out how to check group membership.
        self.raiseSkip('Not supported by RabbitMQ')
