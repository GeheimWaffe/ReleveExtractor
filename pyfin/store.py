import pandas as pd
import pathlib
import pyfin.odfpandas as op

def store_frame(df: pd.DataFrame, target_folder_file: list, target_folder_excluded: list, target_folder_anterior: list):
    # save the correct rows
    df.loc[(df['excluded'] == False) & (df['DateFilter'] == 'Current')].drop(['excluded', 'DateFilter'], axis=1).to_csv(pathlib.Path.home().joinpath(*target_folder_file))
    # save the excluded rows somewhere else
    df.loc[(df['excluded'] == True) & (df['DateFilter'] == 'Current')].drop(['excluded', 'DateFilter'], axis=1).to_csv(pathlib.Path.home().joinpath(*target_folder_excluded))
    # save the anterior rows
    df.loc[df['DateFilter'] == 'Previous'].drop(['excluded', 'DateFilter'], axis=1).to_csv(pathlib.Path.home().joinpath(*target_folder_anterior))

def store_frame_to_ods(df: pd.DataFrame, comptes_path: list, comptes_sheet: str):
    # reconvert the date column to date time
    df['Date'] = pd.to_datetime(df['Date'])

    odsfile = pathlib.Path.home().joinpath(*comptes_path)
    wb = op.SpreadsheetWrapper()
    wb.load(odsfile)
    # get the sheet
    ws: op.SheetWrapper
    ws = wb.get_sheets().get(comptes_sheet)
    if not ws is None:
        ws.insert_from_dataframe(df, include_headers=False, mode='append')
        wb.save(odsfile.parent.joinpath('testcomptes.ods'))
