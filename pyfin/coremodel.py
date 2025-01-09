# Definition of an abstract class as a converter pattern
import pandas as pd
import datetime as dt
import re
from numpy import round

from pyfin.database import MapOrganisme, MapCategorie

# TODO declare a keyword for the last update column name

class Extractor:
    """ Abstract base class which implements the required methods"""

    @property
    def name(self) -> str:
        return self.__account_name__

    def __init__(self, account_name: str, endpoint: str, archivepoint: str):
        self.__account_name__ = account_name
        self.__endpoint__ = endpoint
        self.__archivepoint__ = archivepoint

    def get_data(self) -> pd.DataFrame:
        return pd.DataFrame()

    def flush(self) -> bool:
        return True


def get_interval(interval_type: str, interval_count: int):
    """ calculating the interval"""
    end_date = dt.date.today()
    start_date = dt.date.today()

    if interval_type == 'week':
        start_date = end_date - dt.timedelta(days=(end_date.isoweekday() - 1) +
                                                  7 * (interval_count - 1))
    elif interval_type == 'day':
        start_date = end_date - dt.timedelta(days=interval_count)

    return start_date, end_date


def set_exclusion(df: pd.DataFrame, exclusion_list: []) -> pd.DataFrame:
    df['excluded'] = df['Description'].apply(lambda x: any([e in x for e in exclusion_list]))
    return df


def extract_numero_cheque(libelle: str) -> str:
    extract = re.findall('[0-9]{7}', libelle)
    if re.match('.*Cheque Emis', libelle) and len(extract) > 0:
        return extract[0]
    else:
        return ''


def parse_numero_cheque(ds: pd.Series) -> pd.Series:
    return ds.apply(extract_numero_cheque)


def format_description(ds: pd.Series) -> pd.Series:
    return ds.str.title()


def add_extra_columns(df: pd.DataFrame) -> pd.DataFrame:
    df['Economie'] = ''
    df['Réglé'] = ''
    df['Mois'] = df['Date'] + pd.offsets.MonthEnd(0) - pd.offsets.MonthBegin(1)
    return df


def concat_frames(frame_list: list, headers: list) -> pd.DataFrame:
    harmonized_frames = [f[headers] for f in frame_list]
    result = pd.concat(harmonized_frames)
    return result


def set_index(columnname: str, start_index: int, df: pd.DataFrame) -> pd.DataFrame:
    """ Ajoute un index au dataframe"""
    df[columnname] = range(start_index, start_index + len(df))
    return df


def remove_zeroes(column_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """ Enlève les zéros de la colonne"""
    df[column_name].replace(0, None, inplace=True)
    return df


def filter_by_date(df: pd.DataFrame, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    # filter
    df['DateFilter'] = 'Previous'
    df.loc[(df['Date'] >= start_date) & (df['Date'] <= end_date), 'DateFilter'] = 'Current'

    # end
    return df


def add_insertdate(df: pd.DataFrame, insertdate: dt.date) -> pd.DataFrame:
    # set the current date
    df['InsertDate'] = insertdate
    # end
    return df


def map_categories(df: pd.DataFrame, categories: []) -> pd.DataFrame:
    """ This function assumes the Catégorie column already exists"""
    m: MapCategorie

    for m in categories:
        df.loc[df['Description'].str.contains(m.keyword), 'Catégorie'] = m.categorie

    return df


def map_organismes(df: pd.DataFrame, organismes: []) -> pd.DataFrame:
    # Create the column
    df['Organisme'] = ''
    m: MapOrganisme

    for m in organismes:
        df.loc[df['Description'].str.contains(m.keyword), 'Organisme'] = m.organisme

    return df


def get_transaction_description(df: pd.DataFrame) -> pd.Series:
    return df.apply(lambda x: f'Transaction {x.Description} à date du {x.Date.strftime("%d/%m/%Y")}', axis=1)


def breakdown_value(value: float, periods) -> []:
    howmany = int(periods)
    if howmany > 1:
        return [round(value / float(howmany), 2)] * (howmany - 1) + [
            round(value - (float(howmany) - 1) * round(value / float(howmany), 2), 2)]
    else:
        return value


def breakdown_period(value: dt.date, periods) -> []:
    howmany = int(periods)
    if howmany > 1:
        return [dt.date(value.year, i, 1) for i in range(1, howmany + 1)]
    else:
        return value


def explode_values(df: pd.DataFrame, value_column: str, period_column: str, indexes) -> pd.DataFrame:
    # set the periodizations
    percol = 'periodize'
    df[percol] = 1
    df.loc[indexes, percol] = 12
    df[value_column] = df.apply(lambda x: breakdown_value(x[value_column], x[percol]), axis=1)
    df[period_column] = df.apply(lambda x: breakdown_period(x[period_column], x[percol]), axis=1)
    df = df.explode([value_column, period_column])
    df = df.drop(percol, axis=1)
    return df


def split_dataframes(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # save the correct rows
    current = df.loc[(df['excluded'] == False) & (df['DateFilter'] == 'Current')].drop(['excluded', 'DateFilter'],
                                                                                       axis=1)
    # save the excluded rows somewhere else
    excluded = df.loc[(df['excluded'] == True) & (df['DateFilter'] == 'Current')].drop(['excluded', 'DateFilter'],
                                                                                       axis=1)
    # save the anterior rows
    anterior = df.loc[df['DateFilter'] == 'Previous'].drop(['excluded', 'DateFilter'], axis=1)
    # end of the function
    return current, excluded, anterior

def filter_dataframe_on_date(df: pd.DataFrame, value: dt.date) -> pd.DataFrame:
    return df.loc[df['Date'] == value]

def convert_last_updates_to_frame(last_updates: set) -> pd.DataFrame:
    """ Takes a set of tuples of type (date, text) and converts them to a dataframe"""
    df = pd.DataFrame(data=last_updates,columns=['LastUpdate', 'Compte'])

    return df