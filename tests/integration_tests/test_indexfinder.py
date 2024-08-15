from unittest import TestCase
from pyfin.indexfinder import get_latest_file
from pyfin.indexfinder import get_lastdate_from_file
from pathlib import Path


class Test(TestCase):
    def test_get_latest_file(self):
        latest_file = get_latest_file(Path('..'))
        self.assertIsNotNone(latest_file, 'Could not find a valid file')

    def test_get_lastdate_from_file(self):
        latest_file = get_latest_file(Path('/home/vincent/Extracts'))
        last_date = get_lastdate_from_file(latest_file)
        self.assertIsNotNone(last_date, 'Could not find a valid date')