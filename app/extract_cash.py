import pygsheets
import pandas as pd
import datetime as dt
import numpy as np

def extract_cash_info() -> pd.DataFrame:
    pg = pygsheets.authorize(service_account_file='/home/vincent/Credentials/le-projet-de-vincent-ae15acfb90cd.json')
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
    begin_of_week = dt.date.today()
    begin_of_week = begin_of_week - dt.timedelta(days=begin_of_week.isoweekday() - 1)

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

    # filter the raw frame
    raw_frame = raw_frame[raw_frame['Date'] >= begin_of_week]

    raw_frame = raw_frame[['Date', 'Description', 'Dépense', 'N° de référence',
             'Recette', 'Taux de remboursement', 'Compte', 'Catégorie']]

    raw_frame['excluded'] = False

    # sort ascending
    raw_frame = raw_frame.sort_values(by='Date')

    return raw_frame


