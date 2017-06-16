asgi_rabbitmq
=============

RabbitMQ backend for ASGI.

ASGI is a standard interface between network protocol servers
(particularly web servers) and Python applications (Django Channels),
intended to allow handling of multiple common protocol styles
(including HTTP, HTTP2, and WebSocket).

Channels loads into Django as a pluggable app to bring WebSocket,
long-poll HTTP, task offloading and other asynchrony support to your
code, using familiar Django design patterns and a flexible underlying
framework that lets you not only customize behaviours but also write
support for your own protocols and needs.

.. figure:: img/infrastructure.png

.. toctree::
    :maxdepth: 2
    :caption: Contents:

    installation
    usage
    development/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
