from unittest import TestCase
from pyfin.indexfinder import get_latest_file
from pathlib import Path
class Test(TestCase):
    def test_get_latest_file(self):
        latest_file = get_latest_file(Path('.'))
        self.assertIsNotNone(latest_file, 'Could not find a valid file')

