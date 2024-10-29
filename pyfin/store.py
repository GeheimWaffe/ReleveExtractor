import pandas as pd
import pathlib
import pyfin.odfpandas as op
from sqlalchemy import engine

def store_frame(current: pd.DataFrame, excluded: pd.DataFrame, anterior: pd.DataFrame, target_folder_file: list, target_folder_excluded: list, target_folder_anterior: list):
    # save the correct rows
    current.to_csv(pathlib.Path.home().joinpath(*target_folder_file))
    # save the excluded rows somewhere else
    excluded.to_csv(pathlib.Path.home().joinpath(*target_folder_excluded))
    # save the anterior rows
    anterior.to_csv(pathlib.Path.home().joinpath(*target_folder_anterior))

def store_frame_to_ods(insertable: pd.DataFrame, odsfile: pathlib.Path, comptes_sheet: str):
    if len(insertable)>0:
        # reconvert the date column to date time
        for column in  ['Date', 'Mois', 'InsertDate']:
            insertable[column] = pd.to_datetime(insertable[column])
        # and the values to floats
        for column in ['Dépense', 'Recette']:
            insertable[column] = insertable[column].astype(float)

        wb = op.SpreadsheetWrapper()
        wb.load(odsfile)
        # get the sheet
        ws: op.SheetWrapper
        ws = wb.get_sheets().get(comptes_sheet)
        if not ws is None:
            ws.insert_from_dataframe(insertable, include_headers=False, mode='append')
            wb.save(odsfile)
        else:
            raise KeyError(f'sheet {comptes_sheet} not found in the workbook')

def store_frame_to_sql(insertable: pd.DataFrame, e: engine, table: str):
    # remap the columns
    print(insertable.columns)
    insertable.rename(columns={'InsertDate': "Date insertion",
                               'Index': 'No'}, inplace=True)

    # Define expected columns
    expected = ['No',
                'Date',
                'Description',
                'Recette',
                'Dépense',
                'Compte',
                'Catégorie',
                'Mois',
                'Date insertion',
                'Numéro de référence',
                'Organisme'
    ]

    insertable = insertable[expected]

    print(insertable.columns)

    # set the index
    insertable.set_index('No', inplace=True)

    result = int(insertable.to_sql(table, e, if_exists='append'))