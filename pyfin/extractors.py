# Extractor : Crédit Agricole
# Contains the specific extractors, complying with the abstract class
import pandas as pd
from pathlib import Path
from pyfin.coremodel import Extractor
import re
import openpyxl
import pygsheets
import numpy as np
import os
import datetime as dt


class ExtractorCreditAgricole(Extractor):
    # Implemented interfaces
    def __init__(self, endpoint: str, archivepoint: str):
        super().__init__('Crédit Agricole', endpoint, archivepoint)
        self.__files__ = []

    def get_data(self) -> pd.DataFrame:
        files = self.get_downloaded_releve(self.__endpoint__)
        dataframes = []
        # Iterating over the files
        for f in files:
            if f is None:
                pass
            else:
                book: openpyxl.Workbook
                book = openpyxl.open(f, read_only=True)

                names = book.sheetnames
                content = book[names[0]]

                # goal create a clean table of values
                df: pd.DataFrame
                headers = []
                values = []
                start_of_values = False
                for v in content.values:
                    if start_of_values:
                        values.append(list(v))
                    if v[0] == 'Date':
                        headers = list(v)
                        start_of_values = True
                # create the dataframe
                dataframes += [pd.DataFrame(data=values, columns=headers)]

        # concatenating and cleaning
        if len(dataframes) > 0:
            df = pd.concat(dataframes)
            df = self.clean_releve_ca(df)
            self.__files__ = files
            # end
            return df

    def clean_releve_ca(self, raw_frame: pd.DataFrame) -> pd.DataFrame:
        # Transformations
        # replace the backslash characters
        raw_frame['Libellé'] = raw_frame['Libellé'].replace(r'\n', '', regex=True)
        raw_frame['Libellé'] = raw_frame['Libellé'].replace(r'\s{2,}', ' ', regex=True)

        # transform the columns
        raw_frame = raw_frame.rename(columns={'Libellé': 'Description', 'Débit euros': 'Dépense',
                                              'Crédit euros': 'Recette'})

        raw_frame['N° de référence'] = ''
        raw_frame['Taux de remboursement'] = ''
        raw_frame['Compte'] = self.__account_name__
        raw_frame['Catégorie'] = ''
        raw_frame['Date'] = raw_frame['Date'].dt.date
        raw_frame['Index'] = ''
        # manage exclusions
        raw_frame['excluded'] = False

        return raw_frame

    # Custom functions

    def get_downloaded_releve(self, endpoint: str) -> []:
        downloads = Path.home().joinpath(endpoint)
        f: Path
        return [f for f in downloads.iterdir() if re.match(r'CA\d{8}_\d{6}', f.name)]

    def flush(self) -> bool:
        archivefolder = Path.home().joinpath(self.__endpoint__, self.__archivepoint__)
        if not archivefolder.exists():
            os.mkdir(archivefolder)
        for f in self.__files__:
            os.rename(f, archivefolder.joinpath(f.name))
        # Flush complete, remove the files
        self.__files__ = None
        return True


class ExtractorBoursorama(Extractor):
    def __init__(self, endpoint: str, archivepoint: str):
        super().__init__('Boursorama', endpoint, archivepoint)
        self.__files__ = []

    def get_data(self) -> pd.DataFrame:
        self.__files__ = self.get_downloaded_releve(self.__endpoint__)
        result = []
        for f in self.__files__:
            df = pd.read_csv(f, sep=';', quotechar='"', thousands=' ', decimal=',',
                             parse_dates=['dateOp', 'dateVal'])
            result += [df]

        if len(result) > 0:
            df = pd.concat(result)
            return self.clean_releve_ba(df)

    def clean_releve_ba(self, raw_frame: pd.DataFrame) -> pd.DataFrame:
        # Transformations
        raw_frame['Dépense'] = raw_frame['amount'].where(raw_frame['amount'] < 0, 0).abs()
        raw_frame['Recette'] = raw_frame['amount'].where(raw_frame['amount'] > 0, 0)
        raw_frame['label'] = raw_frame['label'].str.title()

        # transform the columns
        raw_frame = raw_frame.rename(columns={'label': 'Description', 'dateOp': 'Date'})

        raw_frame['N° de référence'] = ''
        raw_frame['Taux de remboursement'] = ''
        raw_frame['Compte'] = self.__account_name__
        raw_frame['Catégorie'] = ''
        raw_frame['Date'] = raw_frame['Date'].dt.date
        raw_frame['Index'] = ''
        # manage exclusions
        raw_frame['excluded'] = False

        return raw_frame

    def get_downloaded_releve(self, endpoint: str) -> []:
        downloads = Path.home().joinpath(endpoint)
        f: Path
        return [f for f in downloads.iterdir() if re.match(r'export-operations', f.name)]

    def flush(self) -> bool:
        archivefolder = Path.home().joinpath(self.__endpoint__, self.__archivepoint__)
        if not archivefolder.exists():
            os.mkdir(archivefolder)
        for f in self.__files__:
            os.rename(f, archivefolder.joinpath(f.name))
        # flush complete, cleaning the files
        self.__files__ = None
        return True


class ExtractorLiquide(Extractor):
    def __init__(self, account_name: str, endpoint: str, archivepoint: str, authentication_key: str):
        super().__init__(account_name, endpoint, archivepoint)
        self.__authentication_key__ = authentication_key

    def get_data(self) -> pd.DataFrame:
        if self.__authentication_key__ is None or self.__authentication_key__ == '':
            raise ValueError(f'No authentication key found for the extractor {self.name}')

        pg = pygsheets.authorize(service_account_file=self.__authentication_key__)
        # select the worksheet
        wb = pg.open('Dépenses Liquides')
        # get the worksheet
        ws: pygsheets.Worksheet
        # create a dict of the worksheets
        wsdict = {ws.title: ws for ws in wb}
        # select the worksheet
        try:
            ws = wsdict[self.__account_name__]
        except KeyError:
            raise KeyError(f'the sheet corresponding the the account name {self.__account_name__} was not found')
        # extract the dataframe
        cash_df = ws.get_as_df()
        # clean
        result = self.clean_cash_info(cash_df)
        # end
        return result

    def clean_cash_info(self, raw_frame: pd.DataFrame) -> pd.DataFrame:
        # filter after a certain date
        raw_frame['Date'] = pd.to_datetime(raw_frame['Date'], format='%d/%m/%Y')
        raw_frame['Date'] = raw_frame['Date'].dt.date

        # transform the columns into numeric types
        for col in ['Dépense', 'Recette']:
            raw_frame[col] = raw_frame[col].replace(' €', '', regex=True)
            raw_frame[col] = raw_frame[col].replace(r',', '.', regex=True)
            raw_frame[col] = raw_frame[col].replace(r'^\s*$', np.nan, regex=True)
            raw_frame[col] = raw_frame[col].astype(float)

        raw_frame['N° de référence'] = ''
        raw_frame['Taux de remboursement'] = ''
        raw_frame['Compte'] = self.__account_name__
        raw_frame['excluded'] = False
        raw_frame['Index'] = ''

        return raw_frame


class ExtractorTest(Extractor):
    def get_data(self) -> pd.DataFrame:
        """ creates a synthetic dataframe with all possible test cases"""
        headers = ['Date', 'Index', 'Description', 'Dépense', 'N° de référence',
                   'Recette', 'Taux de remboursement', 'Compte', 'Catégorie',
                   'excluded']

        df = pd.DataFrame(columns=headers, data=[])
        # create the test rows
        # Dépense
        df.loc[0, ['Date', 'Description', 'Dépense', 'Compte']] = [dt.date.today(), 'Dépense de test', 45.42344, 'Crédit Agricole']
        # Recette
        df.loc[1, ['Date', 'Description', 'Recette', 'Compte']] = [dt.date.today(), 'Recette de test', 32.43,
                                                                   'Boursorama']
        # Dépense à splitter
        df.loc[2, ['Date', 'Description', 'Dépense', 'Compte']] = [dt.date.today(), 'Dépense à splitter', 1000,
                                                                   'Liquide']
        # Recette à splitter
        df.loc[3, ['Date', 'Description', 'Recette', 'Compte']] = [dt.date.today(), 'Recette à splitter', 1500,
                                                                   'Liquide']

        # retourner le résultat
        return df


def get_extractors(endpoint: str, archivepoint: str, authentification_key: str, test_mode: bool) -> []:
    if test_mode:
        return [ExtractorTest('Test Extractor', endpoint, archivepoint)]
    else:
        return [ExtractorCreditAgricole(endpoint, archivepoint),
                ExtractorBoursorama(endpoint, archivepoint),
                ExtractorLiquide('Liquide Vincent', endpoint, archivepoint, authentification_key),
                ExtractorLiquide('Liquide Aurélie', endpoint, archivepoint, authentification_key)]
