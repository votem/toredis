from tornado.testing import AsyncTestCase
import redis
import time
from toredis.client import Client


class TestClient(AsyncTestCase):
    """ Test the client """

    def test_connect(self):
        client = Client(io_loop=self.io_loop)
        result = {}

        def callback():
            result["connected"] = True
            self.stop()
        client.connect(callback=callback)
        self.wait()
        # blocks
        self.assertTrue("connected" in result)

    def test_set_command(self):
        client = Client(io_loop=self.io_loop)
        result = {}

        def set_callback(response):
            result["set"] = response
            self.stop()

        client.connect()
        client.set("foo", "bar", callback=set_callback)
        self.wait()
        #blocks
        self.assertTrue("set" in result)
        self.assertEqual(result["set"], "OK")
        conn = redis.Redis()
        self.assertEqual(conn["foo"].decode('utf-8'), "bar")

    def test_get_command(self):
        client = Client(io_loop=self.io_loop)
        result = {}

        def get_callback(response):
            result["get"] = response
            self.stop()
        time_string = "%s" % time.time()
        conn = redis.Redis()
        conn["foo"] = time_string
        client.connect()
        client.get("foo", callback=get_callback)
        self.wait()
        #blocks
        self.assertTrue("get" in result)
        self.assertEqual(result["get"], time_string)

    def test_sub_command(self):
        client = Client(io_loop=self.io_loop)
        result = {"message_count": 0}
        conn = redis.Redis()

        def sub_callback(response):
            if response[0] == "subscribe":
                result["sub"] = response
                conn.publish("foobar", "new message!")
            elif response[0] == "message":
                result["message_count"] += 1
                if result["message_count"] < 100:
                    count = result["message_count"]
                    return conn.publish("foobar", "new message %s!" % count)
                result["message"] = response[2]
                self.stop()

        client.connect()
        client.subscribe("foobar", callback=sub_callback)
        self.wait()
        # blocks
        self.assertTrue("sub" in result)
        self.assertTrue("message" in result)
        self.assertTrue(result["message"], "new message 99!")

    def test_pub_command(self):
        client = Client(io_loop=self.io_loop)
        result = {}

        def pub_callback(response):
            result["pub"] = response
            self.stop()
        client.connect()
        client.publish("foobar", "message", callback=pub_callback)
        self.wait()
        # blocks
        self.assertTrue("pub" in result)
        self.assertEqual(result["pub"], 0)  # no subscribers yet

    def test_blpop(self):
        client = Client(io_loop=self.io_loop)
        result = {}

        def rpush_callback(response):
            result["push"] = response

            def blpop_callback(response):
                result["pop"] = response
                self.stop()

            client.blpop("test", 0, blpop_callback)

        client.connect()
        client.rpush("test", "dummy", rpush_callback)
        self.wait()
        self.assertEqual(result["pop"], ["test", "dummy"])

    def test_disconnect(self):
        client = Client(io_loop=self.io_loop)
        client.connect()
        client.close()
        with self.assertRaises(IOError):
            client._stream.read_bytes(1024, lambda x: x)
