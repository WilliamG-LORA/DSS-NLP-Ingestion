import unittest
from lurkers import *

class TestEtnetLurkerMethods(unittest.TestCase):

    def setUp(self):
        self.lurker_hk = Etnet(ticker='700',test_mode=True)
        self.lurker_us = Etnet(ticker='AAPL',test_mode=True)


    def test_canScrapeHK(self):
        self.assertTrue(self.lurker_hk.dryrun())

    def test_canSkipDuplicatedValue(self):
        self.lurker_hk.dryrun()
        if self.lurker_hk.hasScrapedDocument():
            self.assertTrue(self.lurker_hk.getSkippedQueryNum()>0)

    def test_canSkipUS(self):
        self.assertTrue(self.lurker_us.dryrun())    

if __name__ == '__main__':
    unittest.main()
