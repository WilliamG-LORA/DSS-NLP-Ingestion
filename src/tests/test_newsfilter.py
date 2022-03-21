import unittest
from lurkers import *

class TestNewsfilterLurkerMethods(unittest.TestCase):

    def test_canScrapeUS(self):
        lurker = Newsfilter('TSLA')
        self.assertTrue(lurker.dryrun())

    def test_canScrapeHK(self):
        lurker = Newsfilter('700-HK')
        self.assertTrue(lurker.dryrun())

if __name__ == '__main__':
    unittest.main()