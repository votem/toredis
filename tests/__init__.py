import unittest

from tests.test_client import TestClient
from tests.test_handler import TestRedis
from tests.test_pipeline import TestPipeline

TEST_MODULES = [
    "test_client",
    "test_handler",
    "test_pipeline",
]

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestClient))
    suite.addTest(unittest.makeSuite(TestRedis))
    suite.addTest(unittest.makeSuite(TestPipeline))
    return suite
