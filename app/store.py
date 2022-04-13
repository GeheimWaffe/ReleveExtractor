import pandas as pd
import pathlib

def store_frame(dataframes: list, target_folder_file: list,target_folder_excluded: list):
    df: pd.DataFrame
    df = pd.concat(dataframes)
    # save the correct rows
    df[df['excluded'] == False].to_csv(pathlib.Path.home().joinpath(*target_folder_file))
    # save the excluded rows somewhere else
    df[df['excluded'] == True].to_csv(pathlib.Path.home().joinpath(*target_folder_excluded))

def store_frame_to_ods(df: pd.DataFrame, comptes_path: list, comptes_sheet: str):
    with pd.ExcelWriter(pathlib.Path.home().joinpath(*comptes_path), engine='odf', mode='a', if_sheet_exists='replace') as wo:
        df[df['excluded'] == False].to_excel(wo, sheet_name='Import')


