import unittest
import pyfin.odfpandas as op
from pathlib import Path
import datetime as dt
import pandas as pd


class TestSyntheticWorkbook(unittest.TestCase):
    def setUp(self):
        self.workbookpath = Path('fixtures/TestWorkbook.ods')

    def test_row_style(self):
        wb = op.SpreadsheetWrapper()
        wb.load(self.workbookpath)
        ws = wb.get_sheets().get('Feuille2')
        row = ws.get_row(2)
        print(ws.get_row_style_array(row))

    def test_workbook_generation(self):
        wb = op.SpreadsheetWrapper()
        wb.load(self.workbookpath)
        # generate a set of values
        values = [["hello", 1, 4, dt.date(2022, 1, 23), dt.datetime.now()]
            , ["bye", 4, 2.4322, dt.date(2022, 1, 24), dt.datetime.now()]
            , ["welcome", 2, 2.11, dt.date(2022, 1, 31), dt.datetime.now()]]
        # generate a dataframe
        df = pd.DataFrame(values, columns=['Text', 'Integer', 'Float', 'Date', 'CurrentTime'])
        df['Date'] = pd.to_datetime(df['Date'])
        df['pureDate'] = df['Date'].dt.date
        df['CurrentTime'] = pd.to_datetime(df['CurrentTime'])
        df = df[['Text', 'Integer', 'Float', 'pureDate', 'Date', 'CurrentTime']]

        print(df.dtypes)
        # get the sheet
        ws: op.SheetWrapper
        ws = wb.get_sheets().get('Feuille1')
        self.assertFalse(ws is None, 'the worksheet Feuille1 was not found')
        ws.insert_from_dataframe(df, mode='append')
        wb.to_xml()
        wb.save(self.workbookpath)

class TestComptesWorkbook(unittest.TestCase):
    def setUp(self):
        self.workbookpath = Path('fixtures/Comptes_2024.ods')
        self.dataframe = pd.DataFrame(data={"Date": [dt.date.today()],
                                            "N°": [10000],
                                            "Description": ['Test Description'],
                                            "Dépense": [100.25],
                                            "N° de référence": [1000],
                                            "Recette": [0],
                                            "Taux de remboursement": [0.00],
                                            "Compte": ['Crédit Agricole'],
                                            "Catégorie": ['Vacances'],
                                            "Economie": [False],
                                            "Réglé": [False],
                                            "Mois": [dt.date.today()-dt.timedelta(dt.date.today().day-1)],
                                            "Date d'insertion": [dt.datetime.now()]})
        self.dataframe['Date'] = pd.to_datetime(self.dataframe['Date'])
        self.dataframe['Mois'] = pd.to_datetime(self.dataframe['Mois'])

    def test_row_insertion(self):
        wb = op.SpreadsheetWrapper()
        wb.load(self.workbookpath)
        ws: op.SheetWrapper
        ws = wb.get_sheets().get('Mouvements')
        ws.insert_from_dataframe(self.dataframe, mode='append')
        wb.save(self.workbookpath)

if __name__ == '__main__':
    unittest.main()
