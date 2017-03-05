from os import environ
from random import choice
from string import ascii_letters

from rabbitmq_admin import AdminAPI


class RabbitmqLayerTestCaseMixin(object):
    """
    TestCase mixin for Django tests.

    Allow to test your channels project against real broker.  Provide
    isolated virtual host for each test.
    """

    def _pre_setup(self):
        """Create RabbitMQ virtual host."""

        super(RabbitmqLayerTestCaseMixin, self)._pre_setup()
        hostname = environ.get('RABBITMQ_HOST', 'localhost')
        port = environ.get('RABBITMQ_PORT', '5672')
        user = environ.get('RABBITMQ_USER', 'guest')
        password = environ.get('RABBITMQ_PASSWORD', 'guest')
        management_port = environ.get('RABBITMQ_MANAGEMENT_PORT', '15672')
        management_url = 'http://%s:%s' % (hostname, management_port)
        self.virtual_host = ''.join(choice(ascii_letters) for i in range(8))
        self.amqp_url = 'amqp://%s:%s/%s' % (hostname, port, self.virtual_host)
        self.management = AdminAPI(management_url, (user, password))
        self.management.create_vhost(self.virtual_host)
        self.management.create_user_permission(user, self.virtual_host)

    def _post_teardown(self):
        """Remove RabbitMQ virtual host."""

        self.management.delete_vhost(self.virtual_host)
        del self.virtual_host
        del self.amqp_url
        del self.management
        super(RabbitmqLayerTestCaseMixin, self)._post_teardown()
