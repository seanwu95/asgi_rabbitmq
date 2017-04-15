
.. :changelog:

Changelog
---------

0.4.1 (2017-04-15)
++++++++++++++++++

- Add ``__version__`` variable to pass channels package compatibility
  test.

0.4 (2017-04-15)
++++++++++++++++

- New style process local channels support.  This version is
  compatible with ASGI reference >= 1.1
- Declared queues cache was introduced.  Channels worker bootstrap
  hook was removed.
- Python 3.6 and Django 1.11 compatibility.
- Connection and AMQP channels level errors are propagated to the
  caller thread.
- ``TEST_CONFIG`` support for test case mixin.

0.3 (2017-03-28)
++++++++++++++++

- Add ``RabbitmqLocalChannelLayer`` to use RabbitMQ layer together
  with IPC.
- Add ``RabbitmqLayerTestCaseMixin`` to use with Channels live server
  test case.
- Improved thread locking mechanism.
- Cryptography support.
- Layer ``channel_capacity`` option support.

0.2 (2017-01-29)
++++++++++++++++

- Significant speed improvement for layer receive method.

0.1 (2017-01-13)
++++++++++++++++

- Initial public release.
