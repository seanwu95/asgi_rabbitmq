Layer Implementation
====================

Heartbeats
----------

RabbitMQ requires client to respond to the heartbeat frames.  After
twice missed heartbeats server will close connection forcibly.
Channels package is written in the synchronous code completely.  To be
able to handle heartbeats responsively we need the ability to process
connection frames separately from channels.  We need threads.

Threading
---------

We use `pika`_ library as RabbitMQ client.  Its connection object
isn't thread safe.  Obviously we will have one additional thread
holding connection and working event loop.  To share this connection
between worker threads we will introduce lock object.

This lock object is acquired by default.  After event loop fully
started and connection is established it will be released first time.
Also threading event will be sent at this moment to notify worker
threads they can start working with connection.

If worker thread want to call any connection method, it need to
acquire this lock.  If connection receive frame from RabbitMQ, to
process it event look need to acquire this lock too.  For this reason
we introduced ``LayerConnection`` class.

Since AMQP is stateful protocol, worker threads can affect connection
state of another worker.  For this reason each worker has its own
unique AMPQ channel (light connection) inside socket.

Worker threads wait for results from the connection thread.
Communication between threads is done by ``concurrent.Future``
objects.  To pass this future instance between callbacks sequence
``Protocol`` object was introduced.  It holds all rabbitmq operations
related to current thread.  Current future instance is stored as
protocol instance attribute.

.. _pika: http://pika.readthedocs.io/en/latest/
