import threading
import time
from collections import defaultdict

import pytest
from asgi_ipc import IPCChannelLayer
from asgi_rabbitmq import RabbitmqChannelLayer
from asgi_rabbitmq.core import EXPIRE_GROUP_MEMBER
from asgiref.conformance import ConformanceTestCase
from channels.asgi import ChannelLayerWrapper
from channels.routing import null_consumer, route
from channels.worker import Worker
from pika.exceptions import ConnectionClosed


class RabbitmqChannelLayerTest(ConformanceTestCase):

    @pytest.fixture(autouse=True)
    def setup_channel_layer(self, rabbitmq_url):

        self.rabbitmq_url = '%s?heartbeat_interval=%d' % (
            rabbitmq_url, self.heartbeat_interval)
        self.channel_layer = RabbitmqChannelLayer(
            self.rabbitmq_url,
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

    def declare_queue(self, name, arguments=None):
        """Declare queue in current vhost."""

        self.management.post_definitions({
            'queues': [{
                'name': name,
                'vhost': self.virtual_host,
                'durable': False,
                'auto_delete': False,
                'arguments': arguments or {},
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

    def test_receive_from_non_existed_channel(self):
        """
        If we tries to receive messages from channel which queue was not
        declared already, things should works just fine.
        """

        channel, message = self.channel_layer.receive(['foo'])
        assert (channel, message) == (None, None)

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
        time.sleep(self.channel_layer.expiry + 1)
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

    def test_new_channel_declare_queue(self):
        """`new_channel` must declare queue if its name is available."""

        name = self.channel_layer.new_channel('test.foo!')
        queue = name.rsplit('!', 1)[-1]
        assert queue in self.defined_queues

    def test_new_channel_capacity(self):
        """Channel created with `new_channel` must support capacity check."""

        name = self.channel_layer.new_channel('test.foo!')
        for _ in range(self.capacity_limit):
            self.channel_layer.send(name, {'hey': 'there'})
        with self.assertRaises(self.channel_layer.ChannelFull):
            self.channel_layer.send(name, {'hey': 'there'})

    def test_add_reply_channel_to_group(self):
        """
        Reply channel is the most popular candidate for group membership.
        Check we can do it.
        """

        name = self.channel_layer.new_channel('test.foo!')
        self.channel_layer.group_add('test.group', name)
        self.channel_layer.send_group('test.group', {'foo': 'bar'})
        channel, message = self.channel_layer.receive([name])
        assert channel == name
        assert message == {'foo': 'bar'}
        self.channel_layer.group_discard('test.group', name)
        self.channel_layer.send_group('test.group', {'foo': 'bar'})
        channel, message = self.channel_layer.receive([name])
        assert channel is None
        assert message is None

    def test_access_to_the_layer_instance_from_different_threads(self):
        """
        Operations made by one thread shouldn't change connection state
        made from another thread.
        """

        def receive_non_existed():
            time.sleep(0.015)
            self.channel_layer.receive(['bar'])

        thread = threading.Thread(target=receive_non_existed)
        thread.deamon = True
        thread.start()

        # If all things goes fine, this line will be executed
        # successfully.
        self.channel_layer.group_add('my_group', 'foo')

    @pytest.mark.skip(reason='FIXME: hanged test')
    def test_receive_blocking_mode(self):
        """Check we can wait until message arrives and return it."""

        self.declare_queue('foo', {'x-dead-letter-exchange': 'dead-letters'})

        def wait_and_send():
            time.sleep(1)
            self.channel_layer.send('foo', {'bar': 'baz'})

        thread = threading.Thread(target=wait_and_send)
        thread.deamon = True
        thread.start()

        channel, message = self.channel_layer.receive(['foo'], block=True)
        assert channel == 'foo'
        assert message == {'bar': 'baz'}

    def test_send_group_message_expiry(self):
        """
        Tests that messages expire correctly when it was sent to group.
        """
        self.channel_layer.group_add('gr_test', 'me_test')
        self.channel_layer.send_group('gr_test', {'value': 'blue'})
        time.sleep(self.expiry_delay)
        channel, message = self.channel_layer.receive(['me_test'])
        self.assertIs(channel, None)
        self.assertIs(message, None)

    def test_group_add_is_idempotent(self):
        """
        Calling group_add continuously should set system into the right
        state.
        """

        self.channel_layer.group_add('gr_test', 'ch_test')
        self.channel_layer.thread.schedule(
            EXPIRE_GROUP_MEMBER,
            'gr_test',
            'ch_test',
        )
        time.sleep(0.1)
        # NOTE: Implementation detail.  Dead letters consumer should
        # ignore messages died with maxlen reason.  This messages
        # caused by sequential group_add calls.
        self.channel_layer.send_group('gr_test', {'value': 'blue'})
        channel, message = self.channel_layer.receive(['ch_test'])
        assert channel == 'ch_test'
        assert message == {'value': 'blue'}

    def test_connection_on_close_notify_futures(self):
        """
        If connection in the connection thread was closed for some reason
        we should notify waiting thread about this error.
        """

        # Wait for connection established.
        while not self.channel_layer.thread.connection.protocols:
            time.sleep(0.5)
        # Get dead letters future.
        future = self.channel_layer.thread.connection.protocols[None].resolve
        # Look into on_close_callback.
        self.channel_layer.thread.connection.connection.close()
        with pytest.raises(ConnectionClosed):
            future.result()

    def test_deny_schedule_calls_to_the_closed_connection(self):
        """
        If connection is already closed, it shouldn't be possible to call
        layer methods on it.
        """

        # Wait for connection established.
        while not self.channel_layer.thread.connection.connection.is_open:
            time.sleep(0.5)
        # Close connection and wait for it.
        self.channel_layer.thread.connection.connection.close()
        while not self.channel_layer.thread.connection.connection.is_closed:
            time.sleep(0.5)
        # Look into is_closed check.
        with pytest.raises(ConnectionClosed):
            self.channel_layer.send('foo', {'bar': 'baz'})

    def test_resolve_callbacks_during_connection_close(self):
        """
        Connection can be in closing state.  If during this little time
        frame another thread tries to schedule callback into this
        connection, we should interrupt immediately.
        """

        # Wait for connection established.
        while not self.channel_layer.thread.connection.connection.is_open:
            time.sleep(0.5)
        # Try to call layer send right after connection close frame
        # was sent.
        self.channel_layer.thread.connection.connection.close()
        with pytest.raises(ConnectionClosed):
            self.channel_layer.send('foo', {'bar': 'baz'})
