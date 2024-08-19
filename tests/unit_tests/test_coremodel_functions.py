from unittest import TestCase
import pandas as pd
import datetime as dt
from pyfin.coremodel import set_index, explode_values
from pyfin.coremodel import add_insertdate
from pyfin.coremodel import extract_numero_cheque
from pyfin.coremodel import parse_numero_cheque
from pyfin.coremodel import remove_zeroes
from pyfin.coremodel import filter_by_date
from pyfin.coremodel import get_transaction_description
from pyfin.coremodel import breakdown_value
from pyfin.coremodel import breakdown_period
from numpy.random import randint
from numpy.random import rand

class TestCoreModelFunctions(TestCase):

    def setUp(self):
        self.dataframe = pd.DataFrame([['a', 'b', 'c', 0]
                                          , ['g', 'h', 'f', 1]
                                          , ['a', 'b', 'z', 2]
                                          , ['i', 'j', 'k', 0]]
                                      , columns=['col1', 'col2', 'col4', 'numbers'])
        self.dataframe.set_index(
            pd.date_range(start=dt.date.today() - dt.timedelta(days=2), periods=len(self.dataframe), freq='D',
                          name='Date'), inplace=True)
        self.dataframe.reset_index(inplace=True)
        self.dataframe['Date'] = self.dataframe['Date'].dt.date
        self.dataframe['Description'] = ['virement', 'retrait', 'transfert', 'retrait']
        self.dataframe['Mois'] = [dt.date(dt.date.today().year, randint(1, 12), 1)]*len(self.dataframe)
        self.dataframe['Dépense'] = rand(len(self.dataframe))
        self.dataframe['Recette'] = rand(len(self.dataframe))

    def test_extract_numero_cheque(self):
        label = 'Cheque Emis 9355334'
        result = extract_numero_cheque(label)
        self.assertEqual('9355334', result, 'Extraction du numéro de chèque a échoué')

    def test_parse_numero_cheque(self):
        s = pd.Series(['Cheque Emis 1234567', 'Cheque Emis 7654321', 'Courses Leclerc'])
        result = parse_numero_cheque(s)
        self.assertEqual(result[0], '1234567', 'Vérification de la longueur de la série')
        self.assertEqual(result[1], '7654321', 'Vérification de la longueur de la série')

    def test_set_index(self):
        df = set_index('Index', 10, self.dataframe)
        self.assertTrue(df.loc[0, 'Index'] == 10, 'Start index is not 10')
        self.assertTrue(df.loc[1, 'Index'] == 11, 'End index is not 11')

    def test_replace_zeroes(self):
        df = self.dataframe.copy()
        # count the zeroes beforehand
        cz_before = df.loc[df['numbers'] == 0, 'numbers'].count()
        df = remove_zeroes('numbers', df)
        cz_after = len(df.loc[df['numbers'].isna(), 'numbers'])
        self.assertEqual(cz_before, cz_after, 'zeroes were not effectively removed')

    def test_set_date(self):
        df = self.dataframe.copy()
        df = add_insertdate(df, dt.date.today())
        self.assertTrue(df.loc[0, 'InsertDate'] == dt.date.today(), 'insert date could not be set')

    def test_filter_by_date(self):
        df = self.dataframe.copy()
        df = filter_by_date(df, dt.date.today(), dt.date.today() + dt.timedelta(days=30))
        # count the number of rows in the dataframe which are before today
        c_previous = len(df.loc[df['Date'] < dt.date.today()])
        check_previous = df.loc[df['DateFilter'] == 'Previous', 'Date'].count()
        self.assertEqual(c_previous, check_previous, f'incorrect number of previous values')
        c_future = len(df.loc[df['Date'] >= dt.date.today()])
        check_future = df.loc[df['DateFilter'] == 'Current', 'Date'].count()
        self.assertEqual(c_future, check_future, f'incorrect number of current values')

    def test_create_description(self):
        df = self.dataframe.copy()
        s = get_transaction_description(df)
        print(s)
        self.assertIsNotNone(s, f'could not produce the descriptive series')

    def test_breakdown_value(self):
        result = breakdown_value(240.0, 12)
        self.assertIsNotNone(result, f'no array returned')
        self.assertEqual(20, result[0])
        self.assertEqual(12, len(result))

    def test_breakdown_month(self):
        result = breakdown_period(dt.date(2024, 8, 1), 12)
        self.assertIsNotNone(result, f'no array returned')
        self.assertEqual(12, len(result))
        self.assertEqual(dt.date(2024, 1, 1), result[0])

    def test_explode(self):
        df = self.dataframe.copy()
        df = explode_values(df, 'Dépense', 'Mois', [0,2])
        print(df)
        self.assertIsNotNone(df, f'could not compute the exploded dataframe')

    def tearDown(self):
        self.dataframe = None
