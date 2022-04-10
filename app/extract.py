import openpyxl
import pathlib
import pandas as pd
import re
import os
def get_downloaded_releve(home_subfolder: str) -> pathlib.Path:
    downloads = pathlib.Path.home().joinpath(home_subfolder)
    f: pathlib.Path
    for f in downloads.iterdir():
        if re.match(r'CA\d{8}_\d{6}', f.name):
            return f

def archive_releve(f: pathlib.Path, target_folders: list):
    archive_folder = pathlib.Path.home().joinpath(*target_folders)
    if not archive_folder.exists():
        os.mkdir(archive_folder)
    os.rename(f, archive_folder.joinpath(f.name))

def extract_raw_releve(download_folder: str, archive_subfolder: str, archive: bool) -> pd.DataFrame:
    f = get_downloaded_releve(download_folder)
    if f is None:
        print('no file found')
    else:
        print(f'file {f.name} retrieved in {download_folder}')

        book: openpyxl.Workbook
        book = openpyxl.open(f, read_only=True)
        print('workbook opened')
        names = book.sheetnames
        content = book[names[0]]
        print('content extracted')
        # goal create a clean table of values
        pd.set_option('display.max_columns', None)
        df: pd.DataFrame
        headers= []
        values = []
        start_of_values = False
        for v in content.values:
            if start_of_values:
                values.append(list(v))
            if v[0] == 'Date':
                headers = list(v)
                start_of_values = True

        # create the dataframe
        df = pd.DataFrame(data=values, columns=headers)
        print(f'data frame created, {len(values)} values found')
        # archive the file
        if archive:
            archive_releve(f, [download_folder, archive_subfolder])
            print(f'file archived in {archive_subfolder}')
        return df
