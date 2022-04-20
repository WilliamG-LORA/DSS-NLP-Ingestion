import os
import unittest
from historydb.redislease import RedisLease
from utils.general_utils import get_configs
from time import sleep

class TestRedisLeaseMethods(unittest.TestCase):

    def setUp(self):
        
        # Setup Config
        setup_configs = get_configs('res/configs/setup-configs.yaml')

        # Redis Params
        self.REDIS_HOST = os.getenv("REDIS_SERVICE_HOST")
        self.REDIS_HISTORY_DB = 'redis_lease_unittest'

        self._db = RedisLease(name=self.REDIS_HISTORY_DB, host=self.REDIS_HOST)

        # Clean up the database
        self._db.cleanup()

    def test_canAddLease_1(self):
        unique_identifier = '111'
        results = self._db.tryAdd(unique_identifier,lease_secs=3)
        self.assertTrue(results)
    
    def test_canAddLease_2(self):
        unique_identifier = '222'
        results = self._db.tryAdd(unique_identifier,lease_secs=1)
        self.assertTrue(results)

    def test_cannotAddifExist(self):
        unique_identifier = '111'
        results = self._db.tryAdd(unique_identifier,lease_secs=3)
        self.assertFalse(results)

    def test_checkifExist(self):
        sleep(1)
        unique_identifier = '222'
        results = self._db.isExist(unique_identifier)
        self.assertFalse(results)

if __name__ == '__main__':
    unittest.main()