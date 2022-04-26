import unittest
import app.odfpandas as op
import pathlib
import datetime as dt
import pandas as pd

class MyTestCase(unittest.TestCase):
    def test_row_style(self):
        wb = op.SpreadsheetWrapper()
        testfile = pathlib.Path.home().joinpath('Documents', 'TestWorkbook.ods')
        wb.load(testfile)
        ws = wb.get_sheets().get('Feuille2')
        row = ws.get_row(2)
        print(ws.get_row_style_array(row))


    def test_workbook_generation(self):
        wb = op.SpreadsheetWrapper()
        testfile = pathlib.Path.home().joinpath('Documents', 'TestWorkbook.ods')
        wb.load(testfile)
        # generate a set of values
        values = [["hello", 1, 4, dt.date(2022, 1, 23), dt.datetime.now()]
            , ["bye", 4, 2.4322, dt.date(2022, 1, 24), dt.datetime.now()]
            , ["welcome", 2.44, 2.11, dt.date(2022, 1, 31), dt.datetime.now()]]
        # generate a dataframe
        df = pd.DataFrame(values, columns=['Text', 'Integer', 'Float', 'Date', 'CurrentTime'])
        df['Date'] = pd.to_datetime(df['Date'])
        df['CurrentTime'] = pd.to_datetime(df['CurrentTime'])

        print(df.dtypes)
        # get the sheet
        ws: op.SheetWrapper
        ws = wb.get_sheets().get('Feuille1')
        self.assertFalse(ws is None, 'the worksheet Feuille1 was not found')
        ws.insert_from_dataframe(df, mode='append')
        wb.to_xml()
        wb.save(testfile)

if __name__ == '__main__':
    unittest.main()
