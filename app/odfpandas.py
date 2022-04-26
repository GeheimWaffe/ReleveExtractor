from odf.opendocument import OpenDocumentSpreadsheet
from odf.opendocument import load
from odf.element import Element
from odf.table import Table
from odf.table import TableRow
from odf.table import TableCell
from odf.text import P
from pathlib import Path
import datetime as dt
import pandas as pd


def generate_table_cell_text(content: str, stylename: str = '') -> TableCell:
    result = TableCell(valuetype='string')
    if stylename != '':
        result.setAttribute('stylename', stylename)
    result.addElement(P(text=content))
    return result


def generate_table_cell_float(content: float, stylename: str = '') -> TableCell:
    result = TableCell(valuetype='float', value=str(content))
    if stylename != '':
        result.setAttribute('stylename', stylename)
    result.addElement(P(text=str(content)))
    return result


def generate_table_cell_datetime(content: dt.datetime, stylename: str = '') -> TableCell:
    result = TableCell(valuetype='date', datevalue=content.strftime('%Y-%m-%dT%H:%M:%S'))
    if stylename != '':
        result.setAttribute('stylename', stylename)
    result.addElement(P(text=content))
    return result


def get_cell_column_span(cell: Element) -> int:
    span = cell.getAttribute('numbercolumnsrepeated')
    return 1 if span is None else int(span)


def get_element_style(elt: Element) -> str:
    style = elt.getAttribute('stylename')
    return '' if style is None else style


class SheetWrapper:
    __sheet__: Table

    def __init__(self, value: Table):
        self.__sheet__ = value

    @property
    def name(self) -> str:
        return self.__sheet__.getAttribute('name')

    def get_row(self, index: int) -> Element:
        return self.__sheet__.getElementsByType(TableRow)[index]

    def is_row_empty(self, row: Element) -> bool:
        analysis = [c.hasChildNodes() for c in row.childNodes]
        return not any(analysis)

    def insert_from_array(self, table_of_values: list):
        for value_row in table_of_values:
            row = TableRow()
            for value in value_row:
                if isinstance(value, float):
                    row.addElement(generate_table_cell_float(value))
                else:
                    row.addElement(generate_table_cell_text(str(value)))
            # add the row to the table
            self.__sheet__.addElement(row)

    def insert_from_dataframe(self, df: pd.DataFrame, include_headers: bool = False, mode: str = 'overwrite'):
        """ inserts a pandas dataframe into the sheet.

        :param df: the dataframe to insert
        :param include_headers: True to include the dataframe headers in the import
        :param mode: the insertion mode.
            'overwrite' : erases all the rows and writes from scratch in the sheet.
            'append' : finds the first empty row and then appends."""

        empty_row = None
        styles = []
        if mode == 'overwrite':
            # delete all the rows
            rows = self.__sheet__.getElementsByType(TableRow)
            for r in rows:
                self.__sheet__.removeChild(r)
        elif mode == 'append':
            # find the first empty row
            empty_row: TableRow
            rows = self.__sheet__.getElementsByType(TableRow)
            for r in rows:
                if self.is_row_empty(r):
                    empty_row = r
                    break
            # get the row styles
            styles = self.get_row_style_array(empty_row)

        # create the headers
        if include_headers:
            row = TableRow()
            for c in df.columns:
                row.addElement(generate_table_cell_text(c))
            self.__sheet__.insertBefore(row, empty_row)

        # import the values
        for df_row in df.itertuples(index=False):
            row = TableRow()
            for i in range(len(df.columns)):
                if len(styles) > 0:
                    style = styles[min(i, len(styles)-1)]
                else:
                    style = ''
                if df.dtypes[i] == 'float64':
                    row.addElement(generate_table_cell_float(df_row[i], style))
                elif df.dtypes[i] == r'datetime64[ns]':
                    row.addElement(generate_table_cell_datetime(df_row[i], style))
                else:
                    row.addElement(generate_table_cell_text(df_row[i], style))
            # add the row to the table
            self.__sheet__.insertBefore(row, empty_row)

    def get_row_style_array(self, row: Element) -> list:
        """ Function that analyzes a row and returns an array of style names.
        There are as many styles as there are cells"""
        cell: Element
        result = []
        default_style = get_element_style(row)
        for cell in row.getElementsByType(TableCell):
            span = get_cell_column_span(cell)
            if get_element_style(cell) == '':
                style = default_style
            else:
                style = get_element_style(cell)
            result += [style] * span
        return result


class SpreadsheetWrapper:
    __workbook__: OpenDocumentSpreadsheet
    __sheets__ = {}

    def __init__(self):
        self.__workbook__ = OpenDocumentSpreadsheet()

    def load(self, filepath: Path):
        self.__workbook__ = load(filepath)
        for t in self.__workbook__.getElementsByType(Table):
            sheet = SheetWrapper(t)
            self.__sheets__[sheet.name] = sheet

    def to_xml(self):
        self.__workbook__.toXml('odsxml.xml')

    def save(self, filepath: Path):
        self.__workbook__.write(filepath)

    def get_sheets(self) -> dict:
        """ returns the dictionary of the sheets"""
        return self.__sheets__
