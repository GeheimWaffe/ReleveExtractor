from unittest import TestCase
from pyfin.configmanager import AppConfiguration
from pathlib import Path

from pyfin.database import get_map_categories


class TestCategoryMapper(TestCase):
    def setUp(self):
        self.mapping_path = 'fixtures/cat_mappings.csv'
        self.confpath = Path('fixtures/pyfin.conf')

    def test_load_categories(self):
        mcs = get_map_categories()

        self.assertGreater(len(mcs), 0, 'Could not find rows')

    def test_config_manager(self):
        conf = AppConfiguration(self.confpath)
        self.assertIsNotNone(conf.extract_folder, 'extract folder setting not found')

    def test_configured_exclusions(self):
        conf = AppConfiguration(self.confpath)
        exclusions = conf.excluded_keywords
        self.assertIsNotNone(exclusions, 'exclusions not properly initialized')
        self.assertGreater(len(exclusions), 1, 'exclusion list is empty')
        self.assertEqual(exclusions[0], 'REMBOURSEMENT DE PRET', 'first item of the exclusion list is not equal to REMBOURSEMENT DE PRET')

