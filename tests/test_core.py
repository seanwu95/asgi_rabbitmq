import time

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

    def test_group_persistence_message_expiry(self):
        """
        Discard channel from all its groups when first message expires in
        channel.
        """

        # Setup group membership.
        self.skip_if_no_extension('groups')
        self.channel_layer.group_add('tgme_group1', 'tgme_test')
        self.channel_layer.group_add('tgme_group2', 'tgme_test')
        self.channel_layer.send('tgme_test', {'hello': 'world'})
        # Wait until message in the channel expires.
        time.sleep(self.channel_layer.expiry)
        # Channel lost its membership in the group #1.
        self.channel_layer.send_group('tgme_group1', {'hello': 'world1'})
        channel, message = self.channel_layer.receive(['tgme_test'])
        self.assertIs(channel, None)
        self.assertIs(message, None)
        # Channel lost its membership in the group #2.
        self.channel_layer.send_group('tgme_group2', {'hello': 'world2'})
        channel, message = self.channel_layer.receive(['tgme_test'])
        self.assertIs(channel, None)
        self.assertIs(message, None)

    def test_group_channels(self):

        # TODO: figure out how to check group membership.
        self.raiseSkip('Not supported by RabbitMQ')
