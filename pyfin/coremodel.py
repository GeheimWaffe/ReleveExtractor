# Definition of an abstract class as a converter pattern
import pandas as pd
import datetime as dt
import re
class Extractor:
    """ Abstract base class which implements the required methods"""
    @property
    def name(self)->str:
        return ''

    def __init__(self, account_name: str, endpoint: str, archivepoint: str):
        self.__account_name__ = account_name
        self.__endpoint__ = endpoint
        self.__archivepoint__ = archivepoint

    def get_data(self)->pd.DataFrame:
        return None

    def flush(self)->bool:
        return True
def validate_schema(self, df: pd.DataFrame):
    pass

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

def set_exclusion(df: pd.DataFrame, exclusion_list:[])->pd.DataFrame:
    df['excluded'] = df['Description'].apply(lambda x: any([e in x for e in exclusion_list]))
    return df

def extract_numero_cheque(libelle: str) -> str:
    extract = re.findall('[0-9]{7}', libelle)
    if re.match('Cheque Emis', libelle) and len(extract) > 0:
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

def set_index(index_name: str, start_index: int, df: pd.DataFrame) -> pd.DataFrame:
    """ Ajoute un index au dataframe"""
    df['Index'] = range(start_index, start_index + len(df))
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
    for m in categories:
        label = m[0]
        categorie = m[1]
        df.loc[df['Description'].str.contains(label), 'Catégorie'] = categorie

    return df