import unittest
from lurkers import *

class TestEastMoneyLurkerMethods(unittest.TestCase):

    def test_canScrapeHK(self):
        lurker = EastMoney(1)
        self.assertTrue(lurker.dryrun())

if __name__ == '__main__':
    unittest.main()
