import openpyxl
import pathlib
import pandas as pd
import re
import os
import datetime as dt

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

def extract_releve_ca(download_folder: str, archive_subfolder: str, archive: bool) -> pd.DataFrame:
    f = get_downloaded_releve(download_folder)
    if f is None:
        pass
    else:
        book: openpyxl.Workbook
        book = openpyxl.open(f, read_only=True)

        names = book.sheetnames
        content = book[names[0]]

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

        # archive the file
        if archive:
            archive_releve(f, [download_folder, archive_subfolder])
            print(f'file archived in {archive_subfolder}')
        return df

def clean_releve_ca(raw_frame: pd.DataFrame, exclusion_list: list) -> pd.DataFrame:
    # Transformations
    # replace the backslash characters
    raw_frame['Libellé'] = raw_frame['Libellé'].replace(r'\n', '', regex=True)
    raw_frame['Libellé'] = raw_frame['Libellé'].replace(r'\s{2,}', ' ', regex=True)

    # transform the columns
    raw_frame = raw_frame.rename(columns={'Libellé': 'Description', 'Débit euros': 'Dépense',
                    'Crédit euros': 'Recette'})

    raw_frame['N° de référence'] = ''
    raw_frame['Taux de remboursement'] = ''
    raw_frame['Compte'] = 'Crédit Agricole'
    raw_frame['Catégorie'] = ''
    raw_frame['Date'] = raw_frame['Date'].dt.date

    raw_frame = raw_frame[['Date', 'Description', 'Dépense', 'N° de référence',
             'Recette', 'Taux de remboursement', 'Compte', 'Catégorie']]

    # capitalize the sentences
    raw_frame['Description'] = raw_frame['Description'].str.title()

    # manage exclusions
    raw_frame['excluded'] = False
    for exclusion in exclusion_list:
        raw_frame['excluded'] = raw_frame['excluded'] | raw_frame['Description'].str.contains(exclusion)

    return raw_frame
