import pathlib
import pandas as pd
import re
import os


def get_downloaded_releve(home_subfolder: str) -> pathlib.Path:
    downloads = pathlib.Path.home().joinpath(home_subfolder)
    f: pathlib.Path
    for f in downloads.iterdir():
        if re.match(r'export-operations', f.name):
            return f


def archive_releve(f: pathlib.Path, target_folders: list):
    archive_folder = pathlib.Path.home().joinpath(*target_folders)
    if not archive_folder.exists():
        os.mkdir(archive_folder)
    os.rename(f, archive_folder.joinpath(f.name))


def extract_releve_ba(download_folder: str, archive_subfolder: str, archive: bool) -> pd.DataFrame:
    file = get_downloaded_releve(download_folder)

    if file is None:
        pass
    else:
        df = pd.read_csv(file, sep=';', quotechar='"', thousands=' ', decimal=',', parse_dates=['dateOp', 'dateVal'])

        # archive the file
        if archive:
            archive_releve(file, [download_folder, archive_subfolder])
            print(f'file archived in {archive_subfolder}')

        return df


def clean_releve_ba(raw_frame: pd.DataFrame) -> pd.DataFrame:
    # Transformations
    raw_frame['Dépense'] = raw_frame['amount'].where(raw_frame['amount'] < 0, 0).abs()
    raw_frame['Recette'] = raw_frame['amount'].where(raw_frame['amount'] > 0, 0)
    raw_frame['label'] = raw_frame['label'].str.title()

    # transform the columns
    raw_frame = raw_frame.rename(columns={'label': 'Description', 'dateOp': 'Date'})

    raw_frame['N° de référence'] = ''
    raw_frame['Taux de remboursement'] = ''
    raw_frame['Compte'] = 'Boursorama'
    raw_frame['Catégorie'] = ''
    raw_frame['Date'] = raw_frame['Date'].dt.date

    raw_frame = raw_frame[['Date', 'Description', 'Dépense', 'N° de référence',
                           'Recette', 'Taux de remboursement', 'Compte', 'Catégorie']]

    # manage exclusions
    raw_frame['excluded'] = False

    return raw_frame
