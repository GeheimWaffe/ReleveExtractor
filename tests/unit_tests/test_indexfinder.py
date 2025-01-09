from unittest import TestCase


from pyfin.indexfinder import get_index_from_database, get_lastdate_from_database
from pyfin.database import get_finance_engine

class Test(TestCase):
    def test_get_index_from_sqlite(self):
        e = get_finance_engine()
        index = get_index_from_database(e, 'comptes', )
        print(f'index found : {index}')
        self.assertGreater(index, 0, 'Could not find a proper index in SQLite')

    def test_get_lastdate_from_sqlite(self):
        e = get_finance_engine()
        dt = get_lastdate_from_database(e, 'comptes')
        print(f'date found : {dt}')
