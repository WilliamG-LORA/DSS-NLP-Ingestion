import unittest
from lurkers import *

class TestEtnetLurkerMethods(unittest.TestCase):

    def test_canScrapeHK(self):
        lurker = Etnet('700')
        self.assertTrue(lurker.dryrun())

if __name__ == '__main__':
    unittest.main()
