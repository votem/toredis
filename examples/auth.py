import logging
import tornado.ioloop
from toredis import Client


def test_auth():
    # Assuming that 12345 is your redis pasword
    redis.auth('12345', after_auth)


def after_auth(status):
    print('Authentication status:', status)
    assert status == 'OK'
    io_loop.stop()


if __name__ == "__main__":
    logging.basicConfig()

    io_loop = tornado.ioloop.IOLoop.instance()

    redis = Client()
    redis.connect('localhost', callback=test_auth)
    io_loop.start()
