import unittest
from lurkers import *

class TestAAstocksLurkerMethods(unittest.TestCase):

    def setUp(self):
        self.lurker = AAstocks(ticker='700',test_mode=True)

    def test_canScrapeHK(self):
        self.assertTrue(self.lurker.dryrun())

    def test_canSkipDuplicatedValue(self):
        self.lurker.dryrun()
        if self.lurker.hasScrapedDocument():
            self.assertTrue(self.lurker.getSkippedQueryNum()>0)
            
if __name__ == '__main__':
    unittest.main()
