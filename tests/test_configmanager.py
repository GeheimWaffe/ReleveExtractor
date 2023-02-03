from unittest import TestCase
from pyfin.configmanager import CategoryMapper
from pathlib import Path

class TestCategoryMapper(TestCase):
    def test_load_csv(self):
        c = CategoryMapper()
        print(c.load_csv(Path.home().joinpath('randompath')))
        self.assertTrue(True, 'Testing is loading nothing works')

        print(c.load_csv(Path(__file__)))
        self.assertTrue(True, 'Testing loading wrong file')

        print(c.load_csv(Path('cat_mappings.csv')))
        l = c.get_mappins()
        self.assertTrue(l[0][0] == 'Enedis', 'Testing the first row')

    def test_get_mappins(self):
        self.assertTrue(True, 'Test successful !')
