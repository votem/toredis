"""Async Redis client for Tornado"""

from collections import deque
from functools import partial, wraps
import logging
import socket

import hiredis

from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from tornado import stack_context

from toredis.commands import RedisCommandsMixin


logger = logging.getLogger(__name__)


class Connection(RedisCommandsMixin):

    def __init__(self, redis, on_connect=None):

        self.redis = redis
        self._on_connect = on_connect

        sock = socket.socket(redis._family, socket.SOCK_STREAM, 0)
        stream = IOStream(sock, io_loop=redis._ioloop)
        self.stream = stream
        stream.set_close_callback(self._on_close)
        stream.read_until_close(self.on_close, self._on_read)
        stream.connect(redis._addr, self._on_connect)

        self.reader = hiredis.Reader()

        # all incoming (p)message events are passed to all that callbacks
        self.msg_callbacks = list()

        # incoming (p)subscribe/(p)unsubscribe events are passed to
        # corresponding callback once
        self.sub_callbacks = dict()
        self.psub_callbacks = dict()

        # used in unlock and to detect empty-response (P)UNSUBSCRIBE
        self.channels = set()
        self.patterns = list()

        # to detect pub/sub status
        self.subscriptions = 0

        self.callbacks = deque()

    def _on_connect(self):
        self.redis._shared.append(self)
        if self._on_connect:
            self._on_connect(self)
            self._on_connect = None

    def _on_read(self, data):
        self.reader.feed(data)
        resp = self.reader.gets()
        while resp != False:
            if self.subscriptions:
                self._on_message(*resp)
            else:
                self.redis.ioloop.add_callback(
                    partial(self.callbacks.popleft(), resp)
                )
            resp = self.reader.gets()

    def _on_message(self, group, source, message):
        if group in ('subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe'):
            self.subscriptions = message
        for callback in self.msg_callbacks:
            self.redis.ioloop.add_callback(
                partial(callback, group, source, message)
            )

    def _unsubscribe_callback(self, callback):
        @wraps(callback)
        def wrapper(pong):
            if pong != 'PONG':
                raise Exception('Problems with (P)UNSUBSCRIBE command!')
            callback([])
        return wrapper

    def is_idle(self):
        return len(self.callbacks) == 0

    def is_shared(self):
        return self in self.redis._shared

    def lock(self):
        if not self.is_shared():
            raise Exception('Connection already is locked!')
        self.redis._shared.remove(self)

    def unlock(self, callback=None):

        def cb(resp):
            assert resp == 'OK'
            self.redis._shared.add(self)

        if self.channels or self.patterns:

            if self.channels:
                self.send_message(['UNSUBSCRIBE'])

            if self.patterns:
                self.send_message(['PUNSUBSCRIBE'])

        self.send_message(['SELECT', self.redis._database], cb)

    def add_pubsub_callback(self, callback):
        if callback not in self.msb_callbacks:
            self.msb_callbacks.append(callback)

    def remove_pubsub_callback(self, callback):
        self.msb_callbacks.remove(callback)

    def send_message(self, args, callback=None):

        command = args[0]

        # Do not allow the commands, affecting the execution of other commands,
        # to be used on shared connection.
        if command in ('SUBSCRIBE', 'PSUBSCRIBE', 'WATCH', 'MULTI', 'SELECT'):
            if self.is_shared():
                raise Exception(
                    'Command is not allowed while connection is shared!'
                )
            if command == 'SUBSCRIBE':
                self.channels.update(args[1:])
            elif command == 'PSUBSCRIBE':
                self.patterns.extend(args[1:])

        # (P)UNSUBSCRIBE commands without arguments don't produce any output
        # from redis when connection is not in pubsub mode. So, we have to add a
        # PING command after them and wrap callback to catch its output. This
        # allows to maintain the callback/response queue in the right order.
        # FIXME: are there any other such commands?
        if len(args) == 1 and command == 'UNSUBSCRIBE' and not self.channels:
            logger.warning('UNSUBSCRIBE: not subscribed to any channels!')

        if len(args) == 1 and command == 'PUNSUBSCRIBE' and not self.patterns:
            logger.warning('PUNSUBSCRIBE: not subscribed to any patterns!')

        self.stream.write(self.format_message(args))
        self.callbacks.append(stack_context.wrap(callback))

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

    def close(self):
        self.send_command(['QUIT'])
        if self.is_shared():
            self.lock()

    def _on_close(self):
        if self.is_shared():
            self.lock()


class Redis(RedisCommandsMixin):

    def __init__(self, host="localhost", port=6379, unixsocket=None,
                 database=0, ioloop=None):
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

        self._database = database

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
