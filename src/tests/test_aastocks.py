import unittest
from lurkers import *

class TestAAstocksLurkerMethods(unittest.TestCase):

    def test_canScrapeHK(self):
        lurker = AAstocks('700')
        self.assertTrue(lurker.dryrun())

if __name__ == '__main__':
    unittest.main()
