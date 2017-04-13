Usage
=====

Initial setup
-------------

RabbitMQ layer can be used exactly the same way as any other channel
layer.  Just add it into ``CHANNEL_LAYERS`` config in the Django
settings module.

.. code:: python

    CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'asgi_rabbitmq.RabbitmqChannelLayer',
            # Change according to your project layout:
            'ROUTING': 'myproject.routing.routes',
            'CONFIG': {
                'url': 'amqp://guest:guest@rabbitmq:5672/%2F',
            },
        },
    }

``url`` parameter is required. It must be string contains full
qualified rabbitmq vhost url. ``/`` symbols must be encoded like
``%2F`` sequence.  You can pass additional parameter to the connection
object with additional query string.  For example we can tune
heartbeat frame internals this way::

    amqp://guest:guest@localhost:5672/myvhost?heartbeat_interval=15

Refer `pika URLParameters`_ documentation for complete list of
possible argument list.

``expiry`` optional message expiration time.  Literally a TTL of your
messages. Default to 60.

``group_expiry`` optional group membership expiration.  How long will
it takes to lose membership in a group after last call to
``Group.add``.  Default to one day.

``capacity`` optional number of messages before backpressure mechanism
comes in.  Default to 100.

``channel_capacity`` optional per channels capacity.  Should be a
dictionary with channel name regexp as a key and capacity as a value.
Refer ``channels`` documentation for more complete info.  Default to
``None``.

``symmetric_encryption_keys`` optional encryption keys.  If provided
should me a list of strings.  Each key will be used as a source for
fernet cipher build.  Use it if you want your messages be encrypted.
Only layer instance with same keys will be able to read received
messages successfully.  Default to ``None``.

Production environment
----------------------

Official `production checklist`_ is definitely a good point to start
prepare your infrastructure for real load.

After initial setup you can try to measure HTTP response
characteristics with wrk_ tool.  WebSockets can be tested the same way
with `autobahn testsuite`_ or thor_ tools.  Channels itself contains
benchmark_ tool but it will require some adaption for your project.

Cluster support
---------------

It is possible to use ASGI RabbitMQ layer over cluster.  Only native
Erlang clustering_ mechanism is supported.  It is based on EPMD_ tool
and requires same version of Erlang virtual machine on each cluster
node.  In production environment you should use dynamic DNS service
which has a very short TTL configuration or a plain TCP load balancer
to hide whole cluster behind one url.  Each channels worker or Daphne
process will have this gateway in a single ``url`` argument.

However it is possible to replicate layer among different data
centers.  We are strongly recommended don't use federation_ or shovel_
plugins.  It is better to setup different layers running each in its
own cluster.  Replication time between data centers is too long for
most channels use cases.

You can test whole installation on single node for development
purposes.

Setup cluster from two nodes on the localhost::

    RABBITMQ_NODE_PORT=5672 RABBITMQ_NODENAME=one rabbitmq-server -detached
    RABBITMQ_NODE_PORT=5673 RABBITMQ_NODENAME=two rabbitmq-server -detached
    rabbitmqctl -n two stop_app
    rabbitmqctl -n two join_cluster rabbit@`hostname -s`
    rabbitmqctl -n two start_app

Add both nodes to the channels config

.. code:: python

    CHANNEL_LAYERS = {
        'one': {
            'BACKEND': 'asgi_rabbitmq.RabbitmqChannelLayer',
            'ROUTING': 'myproject.routing.routes',
            'CONFIG': {
                'url': 'amqp://guest:guest@localhost:5672/%2F',
            },
        },
        'two': {
            'BACKEND': 'asgi_rabbitmq.RabbitmqChannelLayer',
            'ROUTING': 'myproject.routing.routes',
            'CONFIG': {
                'url': 'amqp://guest:guest@localhost:5673/%2F',
            },
        },
    }

We will use first node for Daphne process and second node for worker.
You need to specify explicitly what channel layer you want to use for
ASGI server.

.. code:: python

    # myproject/asgi.py
    import os
    from channels.asgi import get_channel_layer

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'myproject.settings')
    channel_layer = get_channel_layer('one')

Now you can run infrastructure on local machine::

    daphne -e tcp:interface=localhost:port=8000 myproject.asgi:channel_layer
    django-admin runworker --layer two

If your open browser tab on the http://localhost:8000/ you should see
your project index page.  As you can see Daphne knows about first node
and worker knows about second node.  Message synchronization
completely done by cluster itself.

Integration tests
-----------------

Channels provides ``ChannelLiveServerTestCase`` for integration
testing.  It requires ``TEST_CONFIG`` key in the ``default`` channel
layer setting. This additional virtual host needs your attention every
time you want to run tests.  Also RabbitMQ layer doesn't provide
``flush`` extension, so one integration test can affect another.  This
is clearly isn't desired behavior for tests.  We provide addition
``RabbitmqLayerTestCaseMixin`` to automate this temporary virtual host
management.

.. code:: python

    import requests
    from asgi_rabbitmq.test import RabbitmqLayerTestCaseMixin
    from channels.test import ChannelLiveServerTestCase

    class IntegrationTest(RabbitmqLayerTestCaseMixin, ChannelLiveServerTestCase):

        def test_http_request(self):
            """Test the ability to send http requests and receive responses."""

            response = requests.get(self.live_server_url)
            self.assertEqual(response.status_code, 200)

This mixin will create new virtual host before each test and remove it
afterwards.  ``TEST_CONFIG`` becomes unnecessary.

.. _pika urlparameters: http://pika.readthedocs.io/en/latest/modules/parameters.html#urlparameters
.. _production checklist: https://www.rabbitmq.com/production-checklist.html
.. _wrk: https://github.com/wg/wrk
.. _autobahn testsuite: https://github.com/crossbario/autobahn-testsuite
.. _thor: https://github.com/observing/thor
.. _benchmark: https://github.com/django/channels/blob/master/testproject/benchmark.py
.. _clustering: https://www.rabbitmq.com/clustering.html
.. _epmd: http://erlang.org/doc/man/epmd.html
.. _federation: https://www.rabbitmq.com/federation.html
.. _shovel: https://www.rabbitmq.com/shovel.html
