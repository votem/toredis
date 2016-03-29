import logging
import tornado.ioloop
from tornado import gen
from toredis import Client

REDIS_USER_STATUS_DB = 1
REDIS_CONFIG = {
    "server": "localhost",
    "port": 6379,
    "password":"",
    "db": REDIS_USER_STATUS_DB,
}


def test_set():
    """
        Run SET command and verify status
    """
    def after_set(status):
        print 'status=', status
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
    print 'result=', result
    assert result == 11
 
    # Get test_key2 value. Redis will return string instead of number.
    result = yield gen.Task(redis.get, 'test_key2')
    print 'result =', result
    assert result == '11'


class ToRedisClient(Client):

    def on_disconnect(self):
        print 'on_disconnect'
        if self.auto_reconnect:
            self._io_loop.call_later(1, self.reconnect)

    def connect(self, 
                host = 'localhost', 
                port = 6379,
                password = '',
                database = '',
                auto_reconnect = True):
        self.host = host
        self.port = port
        self.password = password,
        self.database = database,
        self.auto_reconnect = auto_reconnect
        self.reconnect()

    def reconnect(self):
        try:
            super(ToRedisClient, self).connect(self.host, self.port, self.auth_first)
        except Exception as ex:
            print 'reconnect ex=', ex

        print 'is_connected=', self.is_connected()

    @gen.engine
    def auth_first(self):
        # Authenticate first
        status = yield gen.Task(self.auth, REDIS_CONFIG['password'])
        print 'status=', status
             
        # Select database
        status = yield gen.Task(self.select, REDIS_CONFIG['db'])
        assert status == 'OK'
             
        print 'auth_first Success'
        
        test_set()

if __name__ == "__main__":
    logging.basicConfig()

    redis = ToRedisClient()
    redis.connect(host=REDIS_CONFIG['server'], 
                  port=REDIS_CONFIG['port'],
                  password=REDIS_CONFIG['password'],
                  database=REDIS_CONFIG['db']
                  )

    tornado.ioloop.IOLoop.instance().start()
