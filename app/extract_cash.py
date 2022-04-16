import pygsheets
import pandas as pd
import datetime as dt
import numpy as np
from pathlib import Path
def extract_cash_info(service_account_file: str) -> pd.DataFrame:
    file = Path(service_account_file)
    pg = pygsheets.authorize(service_account_file=service_account_file)
    # select the worksheet
    wb = pg.open('Dépenses Liquides')

    # get the worksheet
    ws: pygsheets.Worksheet
    ws = wb[0]

    # extract the dataframe
    cash_df = ws.get_as_df()

    # end
    return cash_df

def clean_cash_info(raw_frame: pd.DataFrame) -> pd.DataFrame:
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
    raw_frame['Compte'] = 'Liquide Vincent'

    raw_frame = raw_frame[['Date', 'Description', 'Dépense', 'N° de référence',
             'Recette', 'Taux de remboursement', 'Compte', 'Catégorie']]

    raw_frame['excluded'] = False

    return raw_frame


