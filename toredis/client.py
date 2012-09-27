"""Async Redis client for Tornado"""

from collections import deque
import socket

from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from tornado import stack_context

from toredis.commands import RedisCommandsMixin


class Connection(RedisCommandsMixin):

    def __init__(self, redis, on_connect=None):

        self.redis = redis
        self._on_connect = on_connect

        sock = socket.socket(redis._family, socket.SOCK_STREAM, 0)
        self.stream = IOStream(sock, io_loop=redis._ioloop)
        self.stream.set_close_callback(self.on_close)
        self.stream.connect(redis._addr, self.on_connect)

        self.pub_callbacks = list()
        self.channels = set()
        self.patterns = set()

        self.callbacks = deque()

    def on_connect(self):
        self.redis._shared.append(self)
        if self._on_connect:
            self._on_connect(self)
            self._on_connect = None

    def _clear_pubsub(self):

        self.pub_callbacks = list()

        if self.channels:
            self.send_message(['UNSUBSCRIBE'])
            self.channels = set()

        if self.patterns:
            self.send_message(['PUNSUBSCRIBE'])
            self.patterns = set()

    def format_message(self, args):
        l = "*%d" % len(args)
        lines = [l.encode('utf-8')]
        for arg in args:
            arg = arg.encode('utf-8')
            l = "$%d" % len(arg)
            lines.append(l.encode('utf-8'))
            lines.append(arg)
        lines.append(b"")
        return b"\r\n".join(lines)

    def is_idle(self):
        return len(self.callbacks) == 0

    def add_publish_callback(self, callback):
        # XXX: if callback not in self.pub_callbacks: ?
        self.pub_callbacks.append(callback)

    def is_shared(self):
        return self in self.redis._shared

    def lock(self):
        if not self.is_shared():
            raise Exception('Connection already is locked!')
        self.redis._shared.remove(self)

    def unlock(self):
        self._clear_pubsub()
        self.redis._shared.add(self)

    def send_message(self, args, callback=None):

        command = args[0]

        # some stuff to monitor the state of connection
        if command in ('SUBSCRIBE', 'PSUBSCRIBE', 'WATCH', 'MULTI'):

            if self.is_shared():
                raise Exception(
                    'Command is not allowed while connection is shared!'
                )

            if command == 'SUBSCRIBE':
                self.channels.update(args[1:])

            elif command == 'PSUBSCRIBE':
                self.patterns.update(args[1:])

        self.stream.write(self.format_message(args))

        # (P)UNSUBSCRIBE commands don't produce output from redis.
        # So, we have to do not add callbacks for them to maintain the
        # callbacks in right order.
        # FIXME: are there any other such commands?
        if command not in ('UNSUBSCRIBE', 'PUNSUBSCRIBE'):

            @stack_context.wrap
            def process_response(response):
                self.process_response(response, callback)

            self.callbacks.append(process_response)

    def process_response(self, response, callback):
        raise NotImplementedError()

    def close(self):
        self.send_command(['QUIT'])
        if self.is_shared():
            self.lock()

    def on_close(self):
        if self.is_shared():
            self.lock()


class Redis(RedisCommandsMixin):

    def __init__(self, host="localhost", port=6379, unixsocket=None,
                 ioloop=None):
        """
        Create Redis manager instance.

        It allows to execute Redis commands using flexible connection pool.
        Use get_locked_connection() method to get connections, which are able to
        execute transactions and (p)subscribe commands.
        """

        self._ioloop = ioloop or IOLoop.instance()

        if unixsocket is None:
            self._family = socket.AF_INET
            self._addr = (host, port)
        else:
            self._family = socket.AF_UNIX
            self._addr = unixsocket

        self._shared = deque()

    def _get_connection(self, callback):
        if self._shared:
            self._shared.rotate()
            callback(self._shared[-1])
        else:
            with stack_context.NullContext():
                Connection(self, callback)

    def send_message(self, args, callback):
        """
        Send a message to Redis server.

        args: a list of message arguments including command
        callback: a function to which the result would be passed
        """
        def cb(conn):
            conn.send_message(args, callback)
        self._get_connection(cb)

    def get_locked_connection(self, callback):
        """
        Get connection suitable to execute transactions and (p)subscribe
        commands.

        Locks a connection from shared pool and passes it to callback. If
        there are no connections in shared pool, then a new one would be
        created.
        """
        def cb(conn):
            conn.lock()
            callback(conn)
        self._get_connection(cb)
