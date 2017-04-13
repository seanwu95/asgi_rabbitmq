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

Cluster support
---------------

Integration tests
-----------------

.. _pika urlparameters: http://pika.readthedocs.io/en/latest/modules/parameters.html#urlparameters
