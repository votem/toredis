import logging
import tornado.ioloop
from tornado import gen
from toredis import Client


def finish():
    """
        Stop tornado loop and exit from application
    """
    tornado.ioloop.IOLoop.instance().stop()


def test_set():
    """
        Run SET command and verify status
    """
    def after_set(status):
        assert status == 'OK'
        test_get()

    # Connected to redis, set some key
    redis.set('test_key', 'foobar', callback=after_set)


def test_get():
    """
        Run GET command and verify previously set value
    """
    def after_get(status):
        assert status == 'foobar'
        test_gen()

    redis.get('test_key', callback=after_get)


@gen.engine
def test_gen():
    """
        ``gen.engine`` example
    """
    # Set test_key2 with 10
    result = yield gen.Task(redis.set, 'test_key2', 10)
    assert result == 'OK'

    # Increment test_key2.
    result = yield gen.Task(redis.incr, 'test_key2')
    assert result == 11

    # Get test_key2 value. Redis will return string instead of number.
    result = yield gen.Task(redis.get, 'test_key2')
    assert result == '11'

    finish()


if __name__ == "__main__":
    logging.basicConfig()

    redis = Client()
    redis.connect('localhost', callback=test_set)
    tornado.ioloop.IOLoop.instance().start()
