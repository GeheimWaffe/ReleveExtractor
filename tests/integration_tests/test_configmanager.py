from unittest import TestCase
from pyfin.configmanager import CategoryMapper
from pyfin.configmanager import AppConfiguration
from pathlib import Path

class TestCategoryMapper(TestCase):
    def setUp(self):
        self.mapping_path = 'fixtures/cat_mappings.csv'
        self.confpath = Path('fixtures/pyfin.conf')

    def test_load_csv(self):
        c = CategoryMapper()
        print(c.load_csv(Path('wrongpath')))
        self.assertTrue(True, 'Testing is loading nothing works')

        print(c.load_csv(Path(__file__)))
        self.assertTrue(True, 'Testing loading wrong file')

        print(c.load_csv(Path(self.mapping_path)))
        l = c.get_mappins()
        self.assertTrue(l[0][0] == 'Enedis', 'Testing the first row')

    def test_config_manager(self):
        conf = AppConfiguration(self.confpath)
        self.assertIsNotNone(conf.extract_folder, 'extract folder setting not found')

    def test_configured_exclusions(self):
        conf = AppConfiguration(self.confpath)
        exclusions = conf.excluded_keywords
        self.assertIsNotNone(exclusions, 'exclusions not properly initialized')
        self.assertGreater(len(exclusions), 1, 'exclusion list is empty')
        self.assertEqual(exclusions[0], 'REMBOURSEMENT DE PRET', 'first item of the exclusion list is not equal to REMBOURSEMENT DE PRET')

