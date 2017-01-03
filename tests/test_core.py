import time
from collections import defaultdict

import pytest
from asgi_ipc import IPCChannelLayer
from asgi_rabbitmq import RabbitmqChannelLayer
from asgiref.conformance import ConformanceTestCase
from channels.asgi import ChannelLayerWrapper
from channels.routing import null_consumer, route
from channels.worker import Worker


class RabbitmqChannelLayerTest(ConformanceTestCase):

    @pytest.fixture(autouse=True)
    def setup_channel_layer(self, rabbitmq_url):

        rabbitmq_url = '%s?heartbeat_interval=%d' % (rabbitmq_url,
                                                     self.heartbeat_interval)
        self.channel_layer = RabbitmqChannelLayer(
            rabbitmq_url,
            expiry=1,
            group_expiry=2,
            capacity=self.capacity_limit,
        )

    @pytest.fixture(autouse=True)
    def setup_virtual_host(self, virtual_host):

        self.virtual_host = virtual_host

    @pytest.fixture(autouse=True)
    def setup_management(self, management):

        self.management = management

    @property
    def defined_queues(self):
        """Get queue names defined in current vhost."""

        definitions = self.management.get_definitions()
        queue_definitions = defaultdict(set)
        for queue in definitions['queues']:
            queue_definitions[queue['vhost']].add(queue['name'])
        queues = queue_definitions[self.virtual_host]
        return queues

    def declare_queue(self, name):
        """Declare queue in current vhost."""

        self.management.post_definitions({
            'queues': [{
                'name': name,
                'vhost': self.virtual_host,
                'durable': False,
                'auto_delete': False,
                'arguments': {},
            }],
        })

    expiry_delay = 1.1
    capacity_limit = 5
    heartbeat_interval = 15

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

    def test_connection_heartbeats(self):
        """
        We must answer for RabbitMQ heartbeat frames responsively.
        Otherwise connection will be closed by server.
        """

        self.channel_layer.send('x', {'foo': 'bar'})
        channel, message = self.channel_layer.receive(['x'])
        time.sleep(self.heartbeat_interval * 3)
        # Code below will throw an exception if we don't send
        # heartbeat frames during sleep.
        self.channel_layer.send('x', {'baz': 'quux'})
        channel, message = self.channel_layer.receive(['x'])

    @pytest.mark.xfail
    def test_group_channels(self):

        # TODO: figure out how to check group membership.
        super(RabbitmqChannelLayerTest, self).test_group_channels()

    def test_declare_queues_on_worker_ready(self):
        """Declare necessary queues after worker start."""

        wrapper = ChannelLayerWrapper(
            channel_layer=self.channel_layer,
            alias='default',
            # NOTE: Similar to `channels.routing.Router.check_default` result.
            routing=[
                route('http.request', null_consumer),
                route('websocket.connect', null_consumer),
                route('websocket.receive', null_consumer),
            ],
        )
        worker = Worker(channel_layer=wrapper, signal_handlers=False)
        worker.ready()
        assert self.defined_queues.issuperset({
            'http.request',
            'websocket.receive',
            'websocket.connect',
        })

    def test_skip_another_layer_on_worker_ready(self):
        """
        Don't try to declare rabbit queues if worker uses another layer
        implementation.
        """

        wrapper = ChannelLayerWrapper(
            channel_layer=IPCChannelLayer(),
            alias='default',
            # NOTE: Similar to `channels.routing.Router.check_default` result.
            routing=[
                route('http.request', null_consumer),
                route('websocket.connect', null_consumer),
                route('websocket.receive', null_consumer),
            ],
        )
        worker = Worker(channel_layer=wrapper, signal_handlers=False)
        worker.ready()
        assert self.defined_queues.isdisjoint({
            'http.request',
            'websocket.receive',
            'websocket.connect',
        })

    def test_new_channel_collision(self):
        """Test `new_channel` against existing queue."""

        self.declare_queue('test.foo!yWAcqGFzYtEw')
        self.channel_layer.random.seed(0)
        name = self.channel_layer.new_channel('test.foo!')
        assert name != 'test.foo!yWAcqGFzYtEw'

    # FIXME: test_capacity fails occasionally.
    #
    # Maybe first message succeeds to expire so message count don't
    # cross capacity border.
