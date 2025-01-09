from unittest import TestCase
from pyfin.database import get_last_updates_by_account
from pyfin.coremodel import convert_last_updates_to_frame

class Test(TestCase):
    def test_convert_last_updates_to_frame(self):
        last_updates = get_last_updates_by_account()
        df = convert_last_updates_to_frame(last_updates)
        
        print(df)
