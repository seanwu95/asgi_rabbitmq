
.. |travis| image:: https://img.shields.io/travis/proofit404/asgi_rabbitmq.svg?style=flat-square
    :target: https://travis-ci.org/proofit404/asgi_rabbitmq
    :alt: Build Status

.. |codecov| image:: https://img.shields.io/codecov/c/github/codecov/example-python.svg?style=flat-square
    :target: https://codecov.io/gh/proofit404/asgi_rabbitmq
    :alt: Coverage Status

=============
asgi_rabbitmq
=============

|travis| |codecov|

RabbitMQ backend for ASGI.

- `Source Code`_
- `Issue Tracker`_
- `Documentation`_

Installation
------------

You can install the most recent available version from PyPI::

    pip install asgi_rabbitmq

Usage
-----

To use RabbitMQ broker as your channels layer add following lines to
your django settings

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

Now you can use channels project as usual

    daphne myproject.asgi:channel_layer
    django-admin runworker

License
-------

ASGI RabbitMQ layer is offered under 3-terms BSD license.

.. _source code: https://github.com/proofit404/asgi_rabbitmq
.. _issue tracker: https://github.com/proofit404/asgi_rabbitmq/issues
.. _documentation: http://asgi-rabbitmq.readthedocs.io/en/latest/
