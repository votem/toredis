import logging
import tornado.ioloop
from tornado import gen
from toredis import Client


@gen.engine
def test():
    # Authenticate first
    status = yield gen.Task(redis.auth, '12345')
    assert status == 'OK'

    # Select database
    status = yield gen.Task(redis.select, '0')
    assert status == 'OK'

    print('Success')

    io_loop.stop()

if __name__ == "__main__":
    logging.basicConfig()

    io_loop = tornado.ioloop.IOLoop.instance()

    redis = Client()
    redis.connect('localhost', callback=test)
    io_loop.start()
