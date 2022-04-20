import unittest
from lurkers import *

class TestNewsfilterLurkerMethods(unittest.TestCase):

    def setUp(self):
        self.lurker_hk = Newsfilter(ticker='700-HK',test_mode=True)
        self.lurker_us = Newsfilter(ticker='TSLA',test_mode=True)

    def test_canScrapeHK(self):
        self.assertTrue(self.lurker_hk.dryrun())
    
    def test_canScrapeUS(self):
        self.assertTrue(self.lurker_us.dryrun())

    def test_canSkipDuplicatedValue(self):
        self.lurker_us.dryrun()
        if self.lurker_us.hasScrapedDocument():
            self.assertTrue(self.lurker_us.getSkippedQueryNum()>0)

if __name__ == '__main__':
    unittest.main()