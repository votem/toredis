import logging
import socket

from collections import deque

import hiredis

from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from tornado import stack_context

from toredis.commands import RedisCommandsMixin


logger = logging.getLogger(__name__)


class Client(RedisCommandsMixin):
    def __init__(self, on_disconnect=None, io_loop=None):
        self._io_loop = io_loop or IOLoop.instance()
        self._on_disconnect_callback = on_disconnect

        self._stream = None

        self.reader = None
        self.callbacks = deque()

    def connect(self, host='localhost', port=6379, callback=None):
        self.reader = hiredis.Reader()

        # TODO: Configurable sock family
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._stream = IOStream(sock, io_loop=self._io_loop)
        self._stream.read_until_close(self._on_close, self._on_read)
        self._stream.connect((host, port), callback)

    def _on_read(self, data):
        self.reader.feed(data)

        resp = self.reader.gets()

        while resp is not False:
            if self.callbacks:
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
        self._stream.write(self.format_message(args))
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
        self.quit()
        self._stream.close()

    def _on_close(self, data=None):
        if data is not None:
            self._on_read(data)

        # Trigger on_disconnect
        if self._on_disconnect_callback is not None:
            self._on_disconnect_callback(self)
