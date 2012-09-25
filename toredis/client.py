""" A (really) simple async Redis client for Tornado """

import json
import os
import socket

from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from toredis.response import Response, SubscribeResponse


def get_command_handler(command, response_handler):

    def command_handler(self, *args, **kwargs):

        if self._stream is None:
            raise Exception("Cannot call command before connecting.")

        callback = kwargs.get("callback")

        if callback is None:
            raise ValueError("Missing or invalid callback (%s)" % callback)

        args = [command] + list(args)

        messages = ["*%d" % len(args)]

        for arg in args:
            messages.append("$%d" % len(arg))
            messages.append(arg)

        messages.append("")
        message = "\r\n".join(messages).encode('utf-8')

        self.send_message(message, response_handler, callback)

        return self

    return command_handler


def get_commands():
    return json.load(
        open(os.path.join(os.path.dirname(__file__), 'commands.json'))
    )


class client_metaclass(type):

    def __new__(self, cls_name, cls_parents, cls_attrs):

        commands = get_commands()

        # if ever that time come
        #response_handlers_by_group = {
        #    'set': SetResponse,
        #    'hash': HashResponse,
        #    'string': StringResponse,
        #    'transactions': TransactionResponse,
        #    'generic': GenericResponse,
        #    'list': ListResponse,
        #    'server': ServerResponse,
        #    'sorted_set': SortedSetResponse,
        #    'connection': ConnectionResponse,
        #    'scripting': ScriptingResponse,
        #    'pubsub': PubSubResponse
        #}
        #
        #response_handlers = dict(
        #    (cmd, response_handlers_by_group[commands[cmd]['group']])
        #    for cmd in commands
        #)

        cls_attrs.update(dict(
            (cmd.lower(), get_command_handler(
                cmd,
                SubscribeResponse if cmd in [
                    "SUBSCRIBE", "PSUBSCRIBE"
                ] else Response
            ))
            for cmd in commands
        ))

        return type.__new__(self, cls_name, cls_parents, cls_attrs)


class Client(object):
    """ Stupid simple client """

    __metaclass__ = client_metaclass

    def __init__(self, database=0, ioloop=None):
        self._ioloop = ioloop or IOLoop.instance()
        self._database = database
        self._stream = None

    def connect(self, callback=None, host="localhost", port=6379):
        """ Connect to Redis """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._socket = sock
        self._stream = IOStream(sock, io_loop=self._ioloop)
        self._stream.set_close_callback(self._close)
        self._stream.connect((host, port), callback=callback)

    def disconnect(self):
        """ Close connection to Redis. """
        self._stream.close()

    def _close(self):
        """ Detect a close -- overwrite in sub classes if required """
        pass

    def send_message(self, message, response_class, callback):
        """ Send a message to Redis """

        self._stream.write(message, self.write_callback)
        response_class(self._stream, callback)

    def write_callback(self, *args, **kwargs):
        """ Overwrite in sub classes, if required """
        pass
