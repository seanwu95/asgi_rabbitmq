from pika import SelectConnection
from pika.channel import Channel
from pika.connection import LOGGER


class DebugConnection(SelectConnection):
    """Collect statistics about RabbitMQ methods usage on connection."""

    def _create_channel(self, channel_number, on_open_callback):

        LOGGER.debug('Creating channel %s', channel_number)
        return DebugChannel(self, channel_number, on_open_callback)


class DebugChannel(Channel):
    """Collect statistics about RabbitMQ methods usage on channel."""
