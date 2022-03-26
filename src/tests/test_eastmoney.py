import unittest
from lurkers import *

class TestEastMoneyLurkerMethods(unittest.TestCase):

    def setUp(self):
        self.lurker = Reddit(duration_hr=1,offset_hr=1,test_mode=True)

    def test_canScrape(self):
        self.assertTrue(self.lurker.dryrun())
    
    def test_canSkipDuplicatedValue(self):
        self.lurker.dryrun()
        if self.lurker.hasScrapedDocument():
            self.assertTrue(self.lurker.getSkippedQueryNum()>0)

if __name__ == '__main__':
    unittest.main()
