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

Cluster support
---------------

Integration tests
-----------------

.. _pika urlparameters: http://pika.readthedocs.io/en/latest/modules/parameters.html#urlparameters
