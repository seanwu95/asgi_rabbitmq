
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

Installation
------------

You can install recent available version from PyPI::

    pip install asgi_rabbitmq

Usage
-----

Add following lines to your django settings

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

``url`` in the example above must be written according to the `pika
URLParameters`_ documentation.

.. _pika urlparameters: http://pika.readthedocs.io/en/latest/modules/parameters.html#urlparameters
