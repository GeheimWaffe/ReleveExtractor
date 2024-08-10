""" Ce package permet de récupérer un numéro d'index.
La logique est la suivante :
- Appeler la base de données
- récupérer le max index
- le renvoyer
"""
from pathlib import Path
import pandas as pd
import datetime as dt

def get_latest_file(folder: Path)->Path:
    files = [f for f in folder.iterdir() if f.is_file()]
    files.sort(reverse=True, key=lambda file: file.name)
    try:
        return files[0]
    except Exception:
        raise KeyError(f'could not find a file in folder {folder}')

def get_index_from_file(comptes_csv: Path)->int:
    # load the csv file
    df  = pd.read_csv(comptes_csv)
    # search the index column
    try:
        s = df['N°']
    except Exception:
        raise IndexError(f'could not find the index column (N°) in the CSV')

    return int(s.max())

def get_lastdate_from_file(comptes_csv: Path)->dt.date:
    # load the csv file
    df = pd.read_csv(comptes_csv)
    # search the date column
    try:
        s = df['Date d\'insertion']
    except Exception:
        raise IndexError(f'could not find the insert date column in the CSV')

    # convert the date
    s = pd.to_datetime(s).dt.date
    s = s.fillna(dt.date(2000, 1, 1))
    return s.max()

def get_index_from_folder(folder: Path)->int:
    f = get_latest_file(folder)
    index = get_index_from_file(f)
    return index
