from unittest import TestCase
import pandas as pd
import datetime as dt
from pyfin.clean import set_index
from pyfin.clean import add_insertdate
class Test(TestCase):

    def generate_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame([['a', 'b', 'c', 0]
                              , ['g', 'h', 'f', 1]
                              , ['a', 'b', 'z', '2']]
                          , columns=['col1', 'col2', 'col4', 'numbers'])
        return df


    def test_set_index(self):
        print("running test_set_index")
        df = self.generate_dataframe()
        df = set_index('Index', 10, df)
        self.assertTrue(df.loc[0, 'Index'] == 10, 'Start index is not 10')
        self.assertTrue(df.loc[1, 'Index'] == 11, 'End index is not 11')

    def test_replace_zeroes(self):
        print("running test_replace_zeroes")
        df = self.generate_dataframe()

    def test_set_date(self):
        df = self.generate_dataframe()
        df = add_insertdate(df, dt.date.today())
        self.assertTrue(df.loc[0, 'InsertDate'] == dt.date.today(), 'insert date could not be set')

