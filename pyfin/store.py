import math
from datetime import date, timedelta

import pandas as pd
import pathlib

from sqlalchemy.orm import Session

from pyfin.logger import write_log_entry, write_log_section, write_line

import pyfin.odfpandas as op
from sqlalchemy import engine
from pyfin.database import Job, create_new_job_import, get_mouvements, Mouvement, is_equal_amount_compte, \
    get_mouvements_by_account


def store_frame(current: pd.DataFrame, excluded: pd.DataFrame, anterior: pd.DataFrame, target_folder_file: list,
                target_folder_excluded: list, target_folder_anterior: list):
    # save the correct rows
    current.to_csv(pathlib.Path.home().joinpath(*target_folder_file))
    # save the excluded rows somewhere else
    excluded.to_csv(pathlib.Path.home().joinpath(*target_folder_excluded))
    # save the anterior rows
    anterior.to_csv(pathlib.Path.home().joinpath(*target_folder_anterior))


def validate_frame(insertable: pd.DataFrame) -> pd.DataFrame:
    # remap the columns
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
                'Organisme',
                'Déclarant'
                ]

    insertable = insertable[expected]

    # set the index
    insertable.set_index('No', inplace=True)

    # return the result
    return insertable


def cast_as_float(value):
    result: float
    try:
        result = float(value)
        if math.isnan(result):
            result = 0
    except TypeError:
        result = 0
    return result


def cast_as_string(value):
    """ Designed to handle a specificity of pandas : None values are cast to a nan string"""
    return None if value in ('nan', '') else value


def convert_frame_to_mouvements(df: pd.DataFrame, job: Job) -> list:
    # reset the index
    df.reset_index(inplace=True)

    result = [Mouvement(no=row[0],
                        date=row[1],
                        description=row[2],
                        recette=cast_as_float(row[3]),
                        depense=cast_as_float(row[4]),
                        compte=row[5],
                        categorie=row[6],
                        no_de_reference=cast_as_string(row[7]),
                        mois=row[8],
                        organisme=cast_as_string(row[9]),
                        date_insertion=row[10],
                        declarant=cast_as_string(row[11]),
                        job=job
                        )
              for row in zip(df['No'],
                             df['Date'],
                             df['Description'],
                             df['Recette'],
                             df['Dépense'],
                             df['Compte'],
                             df['Catégorie'],
                             df['Numéro de référence'],
                             df['Mois'],
                             df['Organisme'],
                             df['Date insertion'],
                             df['Déclarant']
                             )]
    return result


def store_frame_to_ods(insertable: pd.DataFrame, odsfile: pathlib.Path, comptes_sheet: str):
    if len(insertable) > 0:
        # reconvert the date column to date time
        for column in ['Date', 'Mois', 'InsertDate']:
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
    insertable = validate_frame(insertable)

    # create a new job
    with Session(e) as session:
        j: Job = create_new_job_import()
        session.add(j)
        session.commit()
        # set the job id
        insertable['job_id'] = j.job_id

    result = int(insertable.to_sql(table, e, if_exists='append'))
    return result


def store_frame_to_sql_mode_7(insertable: pd.DataFrame, e: engine, start_date: date,
                              end_date: date, start_index: int, simulate: bool = False, account_name: str = None):
    """ Special mode for importing into the database"""
    # remap the columns
    insertable = validate_frame(insertable)

    # récupérer les mouvements futurs
    sqlcontexte = 'SQL import mode 7'
    if account_name is None:
        write_log_entry(sqlcontexte, f'Warning ! Import is in global mode, not account specific')
        mvt_futurs = get_mouvements(start_date, end_date)
        write_log_entry(sqlcontexte, f'retrieved {len(mvt_futurs)} over the period {start_date}, {end_date}')
    else:
        write_log_entry(sqlcontexte, f'Import is done specifically for the account {account_name}')
        mvt_futurs = get_mouvements_by_account(start_date, end_date, account_name)
        write_log_entry(sqlcontexte,
                        f'retrieved {len(mvt_futurs)} over the period {start_date}, {end_date} for account {account_name}')

    # Créer un job d'import
    importjob = create_new_job_import()

    # iterate over the transactions
    candidates = convert_frame_to_mouvements(insertable, importjob)

    write_log_entry(sqlcontexte, f'{len(candidates)} to check')

    # Start of the mega-check
    with Session(e) as session:
        for i, candidate in enumerate(candidates):
            write_line()
            write_log_section(f'*** Candidate n°{i}***')
            write_log_entry(sqlcontexte, f'checking candidate {candidate}...')
            # Est-ce que c'est un chèque ?
            if candidate.is_cheque():
                # Existe-t-il un mouvement avec ce numéro de chèque ?
                write_log_entry(sqlcontexte, f'the candidate is a cheque (number : {candidate.no_de_reference})')
                cheques = [c for c in mvt_futurs if c.no_de_reference == candidate.no_de_reference]
                if len(cheques) > 0:
                    cheque = cheques[0]
                    write_log_entry(sqlcontexte, f'a corresponding move was found with ID {cheque.index}')
                    cheque.date = candidate.date
                    cheque.label_utilisateur = cheque.description
                    cheque.description = candidate.description
                    candidate.date_out_of_bound = True
                    session.add(cheque)
                    mvt_futurs.remove(cheque)
                    write_log_entry(sqlcontexte, f'reduced size of transactions : {len(mvt_futurs)} actual size')
                else:
                    # aucun chèque correspondant trouvé
                    write_log_entry(sqlcontexte, f'no corresponding cheque found')
            else:
                # Existe-t-il un mouvement de même compte, montant (auquel cas ceci est un virement ou une dépense notée en avance) ?
                similars = [s for s in mvt_futurs if is_equal_amount_compte(s, candidate)]
                if len(similars) > 0:
                    similar = similars[0]
                    write_log_entry(sqlcontexte, f'a similar transaction was found : {similar}')
                    similar.date = candidate.date
                    if similar.label_utilisateur is None:
                        similar.label_utilisateur = similar.description
                    similar.description = candidate.description
                    candidate.date_out_of_bound = True
                    session.add(similar)
                    mvt_futurs.remove(similar)
                    write_log_entry(sqlcontexte, f'reduced size of transactions : {len(mvt_futurs)} actual size')
                else:
                    # aucun mouvement trouvé
                    write_log_entry(sqlcontexte, f'no similar transaction was found')

            # add the candidate to the session
            candidate.no = start_index
            start_index += 1
            session.add(candidate)
            write_log_entry(sqlcontexte, f'candidate added to the session')

        write_log_section('*** Handling stragglers ***')
        # Second loop : remaining mouvements
        for m in mvt_futurs:
            # shift the mouvement to the end of the period
            if m.get_solde() != 0:
                write_log_entry(sqlcontexte, f'future movement found : {m}. Shifting the date...')
                m.date = end_date + timedelta(days=1)
                session.add(m)
            else:
                write_log_entry(sqlcontexte, f'found a mouvement with 0 solde : {m}. Doing nothing.')

        # flush and commit
        session.flush()
        if not simulate:
            # simulation mode is possible
            session.commit()
            write_log_entry(sqlcontexte, f'changes committed to the database')
        else:
            write_log_entry(sqlcontexte, f'no commit, simulation mode')
