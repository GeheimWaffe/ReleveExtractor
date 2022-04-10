import pandas as pd
import pathlib

def store_frame(df: pd.DataFrame, target_folder_file: list,target_folder_excluded: list):
    # save the correct rows
    df[df['excluded'] == False].to_csv(pathlib.Path.home().joinpath(*target_folder_file))
    # save the excluded rows somewhere else
    df[df['excluded'] == True].to_csv(pathlib.Path.home().joinpath(*target_folder_excluded))


