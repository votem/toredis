from toredis.commands import RedisCommandsMixin


class Pipeline(RedisCommandsMixin):
    """
        Redis pipeline class
    """
    def __init__(self, client):
        """
            Constructor

            :param client:
                Client instance
        """

        self._client = client
        self._messages = []

    def send_message(self, args, callback=None):
        """
            Add command to pipeline

            :param args:
                Command arguments
            :param callback:
                Callback
        """
        message = self._client.format_message(args)
        self._messages.append(message)

    def send(self, callback=None):
        """
            Send command pipeline to redis

            :param callback:
                Callback
        """
        messages = self._messages
        self._messages = []
        self._client.send_messages(messages, callback)
