import pandas as pd
import datetime as dt
from pyfin.configmanager import CategoryMapper
from pathlib import Path
import re

def extract_numero_cheque(libelle: str) -> str:
    extract = re.findall('[0-9]{7}', libelle)
    if re.match('Cheque Emis', libelle) and len(extract) > 0:
        return extract[0]
    else:
        return ''

def parse_numero_cheque(ds: pd.Series) -> pd.Series:
    return ds.apply(extract_numero_cheque)

def add_extra_columns(df_list: list) -> list:
    df: pd.DataFrame
    for df in df_list:
        df['Mois'] = df['Date'] + pd.offsets.MonthEnd(0) - pd.offsets.MonthBegin(1)
        df['Economie'] = ''
        df['Réglé'] = ''

    return df_list

def concat_frames(frame_list: list, headers: list) -> pd.DataFrame:
    harmonized_frames = [f[headers] for f in frame_list]
    result = pd.concat(harmonized_frames)
    return result


def filter_by_date(df: pd.DataFrame, interval_type: str, interval_count: int) -> pd.DataFrame:
    first_date = dt.date.today()
    if interval_type == 'week':
        first_date = first_date - dt.timedelta(days=(first_date.isoweekday() - 1) +
                                               7*(interval_count-1))

    # filter
    df = df[df['Date'] >= first_date]

    # end
    return df


def map_categories(df: pd.DataFrame, csv_file: str) -> pd.DataFrame:
    catmap = CategoryMapper()
    csv_path = Path.home().joinpath(csv_file)
    catmap.load_csv(csv_path)
    maps = catmap.get_mappins()
    for m in maps:
        label = m[0]
        categorie = m[1]
        print(f'mapping label : {label} to catégorie: {categorie}')
        df.loc[df['Description'].str.contains(label), 'Catégorie'] = categorie

    return df