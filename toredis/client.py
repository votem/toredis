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

        self._sub_callback = False

    def connect(self, host='localhost', port=6379, callback=None):
        self._reset()

        # TODO: Configurable sock family
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._stream = IOStream(sock, io_loop=self._io_loop)
        self._stream.read_until_close(self._on_close, self._on_read)
        self._stream.connect((host, port), callback=callback)

    def is_idle(self):
        return len(self.callbacks) == 0

    def is_connected(self):
        return bool(self._stream) or not self._stream.closed()

    def send_message(self, args, callback=None):
        # Special case for pub-sub
        cmd = args[0]

        if (self._sub_callback is not None and
            cmd not in ('PSUBSCRIBE', 'SUBSCRIBE', 'PUNSUBSCRIBE', 'UNSUBSCRIBE')):
            raise ValueError('Cannot run normal command over PUBSUB connection')

        # Send command
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

    # Pub/sub commands
    def psubscribe(self, patterns, callback=None):
        self._set_sub_callback(callback)
        super(Client, self).psubscribe(patterns, callback)

    def subscribe(self, channels, callback=None):
        self._set_sub_callback(callback)
        super(Client, self).subscribe(channels, callback)

    def _set_sub_callback(self, callback):
        if self._sub_callback is None:
            self._sub_callback = callback

        assert self._sub_callback == callback

    # Event handlers
    def _on_read(self, data):
        self.reader.feed(data)

        resp = self.reader.gets()

        while resp is not False:
            if self._sub_callback:
                try:
                    self._sub_callback(resp)
                except:
                    logger.exception('SUB callback failed')
            else:
                if self.callbacks:
                    callback = self.callbacks.popleft()
                    if callback is not None:
                        try:
                            callback(resp)
                        except:
                            logger.exception('Callback failed')
                else:
                    logger.debug('Ignored response: %s' % repr(resp))

            resp = self.reader.gets()

    def _on_close(self, data=None):
        if data is not None:
            self._on_read(data)

        # Trigger any pending callbacks
        callbacks = self.callbacks
        self.callbacks = deque()

        if callbacks:
            for cb in callbacks:
                try:
                    cb(None)
                except:
                    logger.exception('Exception in callback')

        # Trigger on_disconnect
        if self._on_disconnect_callback is not None:
            self._on_disconnect_callback(self)

    def _reset(self):
        self.reader = hiredis.Reader()
        self._sub_callback = None
