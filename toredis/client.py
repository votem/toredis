import logging
import socket

from collections import deque

import hiredis

from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from tornado import stack_context

from toredis.commands import RedisCommandsMixin


logger = logging.getLogger(__name__)


class Connection(RedisCommandsMixin):
    def __init__(self, addr, on_connect=None, on_disconnect=None, io_loop=None):
        self._io_loop = io_loop or IOLoop.instance()
        self._on_connect_callback = on_connect
        self._on_disconnect_callback = on_disconnect

        # TODO: Configurable family
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        stream = IOStream(sock, io_loop=self._io_loop)
        self.stream = stream

        stream.read_until_close(self.on_close, self._on_read)
        stream.connect(addr, self._on_connect)

        self.reader = hiredis.Reader()
        self.callbacks = deque()

    def _on_connect(self):
        if self._on_connect_callback is not None:
            self._on_connect_callback(self)

    def _on_read(self, data):
        self.reader.feed(data)

        resp = self.reader.gets()

        while resp is not False:
            callback = self.callbacks.popleft()
            if callback is not None:
                try:
                    callback(resp)
                except:
                    logger.exception('Callback failed')

            resp = self.reader.gets()

    def is_idle(self):
        return len(self.callbacks) == 0

    def send_message(self, args, callback=None):
        self.stream.write(self.format_message(args))
        if callback is not None:
            callback = stack_context.wrap(callback)
        self.callbacks.append(callback)

    def format_message(self, args):
        l = "*%d" % len(args)
        lines = [l.encode('utf-8')]
        for arg in args:
            if not isinstance(arg, basestring):
                arg = str(arg)
            arg = arg.encode('utf-8')
            l = "$%d" % len(arg)
            lines.append(l.encode('utf-8'))
            lines.append(arg)
        lines.append(b"")
        return b"\r\n".join(lines)

    def close(self):
        self.send_command(['QUIT'])
        self.stream.close()

    def _on_close(self, data=None):
        if data is not None:
            self._on_read(data)

        # Trigger on_disconnect
        if self._on_disconnect_callback is not None:
            self._on_disconnect_callback(self)
