import pandas as pd
import datetime as dt

def prepare_dataframe(raw_frame: pd.DataFrame, exclusion_list: list) -> pd.DataFrame:
    begin_of_week = dt.date.today()
    begin_of_week = begin_of_week - dt.timedelta(days=begin_of_week.isoweekday() - 1)

    # Transformations
    # replace the backslash characters
    raw_frame['Libellé'] = raw_frame['Libellé'].replace(r'\n', '', regex=True)
    raw_frame['Libellé'] = raw_frame['Libellé'].replace(r'\s{2,}', ' ', regex=True)
    raw_frame['Date'] = raw_frame['Date'].dt.date

    # filter after a certain date
    raw_frame = raw_frame[raw_frame['Date'] >= begin_of_week]

    # transform the columns
    raw_frame = raw_frame.rename(columns={'Libellé': 'Description', 'Débit euros': 'Dépense',
                    'Crédit euros': 'Recette'})

    raw_frame['N° de référence'] = ''
    raw_frame['Taux de remboursement'] = ''
    raw_frame['Compte'] = 'Crédit Agricole'

    raw_frame = raw_frame[['Date', 'Description', 'Dépense', 'N° de référence',
             'Recette', 'Taux de remboursement', 'Compte']]

    raw_frame = raw_frame.sort_values(by='Date')

    # manage exclusions
    raw_frame['excluded'] = False
    for exclusion in exclusion_list:
        raw_frame['excluded'] = raw_frame['excluded'] | raw_frame['Description'].str.contains(exclusion)

    return raw_frame
