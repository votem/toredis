import unittest

from tests.test_client import TestClient
from tests.test_handler import TestRedis

TEST_MODULES = [
    "test_client",
    "test_handler",
]

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestClient))
    suite.addTest(unittest.makeSuite(TestRedis))
    return suite
