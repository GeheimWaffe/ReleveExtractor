from unittest import TestCase
from datetime import date, timedelta

from pyfin.database import get_mouvements, get_last_updates_by_account


class Test(TestCase):
    def test_get_mouvements(self):
        start_date = date.today() - timedelta(days=15)
        end_date = date.today()

        result = get_mouvements(start_date, end_date, True)

        for r in result:
            print(r)

        self.assertGreater(len(result), 0, 'Could not find transactions')

    def test_get_last_updates_by_account(self):
        last_updates = get_last_updates_by_account()

        self.assertGreater(len(last_updates), 0, 'Could not find last updates by account')
