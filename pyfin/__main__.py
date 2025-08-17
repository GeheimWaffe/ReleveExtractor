import sys
from pathlib import Path

import pandas as pd

from pyfin.coremodel import Extractor, convert_last_updates_to_frame
import datetime as dt
import pyfin.extractors as extractors
import pyfin.coremodel as c
import pyfin.store as s
import pyfin.indexfinder
from pyfin.logger import write_log_entry
from pyfin.logger import write_log_section
from pyfin.configmanager import AppConfiguration
from pyfin.database import get_finance_engine, get_last_updates_by_account, get_map_categories_dataframe
from pyfin.database import get_map_categories


def get_help() -> str:
    result = "Relevé Extractor (c). Following parameters are possible : \n" \
             "-help : print this help" \
             "-no-archive : the files will not be archived in sub-folders\n" \
             "" \
             "following configurations can be made :" \
             "--ods : imports the data into an ODS file" \
             "--sql : imports the data into a SQL database" \
             "--sql2 : imports the data in a refined way into the SQL database" \
             "--interval-type [day|week|month] : sets the interval of data, default : week" \
             "--interval-count [number] : sets the number of intervals, default : 1" \
             "--set-credentials [path] : configures the credentials for accessing google drive" \
             "--list-mappings : list the currently configured mappings" \
             "--no-archive : do not archive the input files" \
             "--get-index: gets the latest index" \
             "--csv-only: exports only to csv" \
             "--test-mode : loads from a fictitious dataset" \
             "--simulate : only simulates the loading, without commit" \
             "--new-mode : new, account-specific loading mechanism"

    return result


def main(args=None, config_file: Path = None):
    """ main function to run the tool"""
    write_log_section('Starting the program')

    intervaltype = 'week'
    intervalcount = 1
    interval_manual_mode = False
    credentials = ''
    get_index = False
    csv_only = False
    testmode = False
    mode = 'none'
    simulate = False
    new_mode = False

    # retrieving the args
    if args is None:
        args = sys.argv[1:]

    # print the args
    write_log_entry(__file__, f'arguments : {args}')

    for i in range(len(args)):
        if args[i] == '--interval-type':
            intervaltype = args[i + 1]
            interval_manual_mode = True
        if args[i] == '--interval-count':
            intervalcount = int(args[i + 1])
            interval_manual_mode = True
        if args[i] == '--set-credentials':
            credentials = args[i + 1]
        if args[i] == '--get-index':
            get_index = True
        if args[i] == '--csv-only':
            csv_only = True
        if args[i] == '--test-mode':
            testmode = True
        if args[i] == '--sql':
            mode = 'sql'
        if args[i] == '--ods':
            mode = 'ods'
        if args[i] == '--sql2':
            mode = 'sql2'
        if args[i] == '--simulate':
            simulate = True
        if args[i] == '--new-mode':
            new_mode = True
        if args[i] == '--help':
            print(get_help())
            return

    if mode == 'none':
        raise ValueError('No mode was selected (ODS, or SQL)')

    # set the options
    archive = not ("--no-archive" in args) and not simulate

    # load config
    appconfig = AppConfiguration(config_file)

    # load category mappings
    write_log_entry(__file__, f'loading the category mappings from the database')
    mapcategories = get_map_categories()
    mapcategoriesdf = get_map_categories_dataframe()
    write_log_entry(__file__, f'category mappings loaded : {len(mapcategories)} found')

    # set the exclusion list
    # Deactivate the exclusion mechanism
    # exclusion_list = appconfig.excluded_keywords
    exclusion_list = []

    # fix the credentials
    if credentials != '':
        appconfig.service_account_key = credentials

    write_log_section('Defining the parameters')
    write_log_entry(__file__, f'archive the data : {archive}')
    write_log_entry(__file__, f' interval type : {intervaltype}')
    write_log_entry(__file__, f' interval count : {intervalcount}')
    write_log_entry(__file__, f' google drive location : {appconfig.service_account_key}')
    write_log_entry(__file__, f'periodization threshold: {appconfig.periodization_threshold}')
    write_log_entry(__file__, f'mode : {mode}')
    write_log_entry(__file__, f'new account-pecific mode activated' if new_mode else f'classic mode')

    # LAUNCH THE IMPORT
    if new_mode:
        import_mode_2(appconfig.tablecomptes, intervaltype, intervalcount,
                      appconfig.download_folder, appconfig.ca_subfolder,
                      appconfig.service_account_key, testmode, exclusion_list, appconfig.mapping_file, mapcategoriesdf,
                      simulate, archive)
    else:
        import_mode_1(get_index, mode, appconfig.extract_folder, appconfig.tablecomptes, intervaltype, intervalcount,
                      interval_manual_mode, appconfig.download_folder, appconfig.ca_subfolder, appconfig.comptes_folder,
                      appconfig.service_account_key, testmode, exclusion_list, appconfig.mapping_file, mapcategories,
                      csv_only, simulate, archive)


def import_mode_1(get_index: bool, mode: str, extract_folder: str, tablecomptes, intervaltype: str, intervalcount: int,
                  interval_manual_mode: bool, download_folder: str, ca_subfolder: str, comptes_folder: str,
                  service_account_key: str,
                  testmode: bool, exclusion_list, mapping_file: str, mapcategories, csv_only: bool,
                  simulate: bool, archive: bool):
    # Create an engine
    finengine = get_finance_engine()

    if get_index:
        # Check the proper mode
        if mode == 'ods':
            lastcompte = pyfin.indexfinder.get_latest_file(Path(extract_folder))
            last_index = pyfin.indexfinder.get_index_from_file(lastcompte)
            write_log_entry(__file__, f'the last index from the file is : {last_index}')
        elif mode == 'sql':
            last_index = pyfin.indexfinder.get_index_from_database(finengine, tablecomptes)
            write_log_entry(__file__, f'the last index from the database is : {last_index}')
    else:
        # Calculate the last index and start, end dates
        write_log_section('Connecting to the previous exports')
        lastcompte = pyfin.indexfinder.get_latest_file(Path(extract_folder))
        # Initialize the values
        start_date, end_date = c.get_interval(interval_type=intervaltype, interval_count=intervalcount)
        start_index = 0
        write_log_entry(__file__, f'start and and date initialized to : {start_date}-{end_date}')
        if lastcompte is None:
            write_log_entry(__file__, f'could not find a proper file in {extract_folder}')
        else:
            # Setting up the start index
            if mode == 'ods':
                try:
                    start_index = pyfin.indexfinder.get_index_from_file(lastcompte)
                    start_index += 1
                    write_log_entry(__file__, f'Index initialized to {start_index}')
                except Exception as e:
                    write_log_entry(__file__, f'Could not find a proper index source in the file  {lastcompte.name}.'
                                              f'Error : {e}'
                                              f'Defaulting to 0 instead')
                    start_index = 0
            if mode == 'sql':
                try:
                    start_index = pyfin.indexfinder.get_index_from_database(finengine, tablecomptes)
                    write_log_entry(__file__, f'Index initialized to {start_index}')
                except Exception as e:
                    write_log_entry(__file__, f'Could not find a proper index source in the file  {lastcompte.name}.'
                                              f'Error : {e}'
                                              f'Defaulting to 0 instead')
                    start_index = 0

            # Setting up the start date
            if mode == 'ods':
                try:
                    if not interval_manual_mode:
                        start_date = pyfin.indexfinder.get_lastdate_from_file(lastcompte)
                        # start_date += dt.timedelta(days=1)
                        write_log_entry(__file__, f'Start date adjusted to {start_date}')
                    else:
                        pass
                except Exception as e:
                    write_log_entry(__file__,
                                    f'Could not find a proper start date in folder {extract_folder}.'
                                    f'Error : {e}'
                                    f'reverting to standard start and end date instead')
            if mode == 'sql' or mode == 'sql2':
                try:
                    if not interval_manual_mode:
                        start_date = pyfin.indexfinder.get_lastdate_from_database(finengine, tablecomptes)
                        # start_date += dt.timedelta(days=1)
                        write_log_entry(__file__, f'Start date adjusted to {start_date}')
                    else:
                        pass
                except Exception as e:
                    write_log_entry(__file__,
                                    f'Could not find a proper start date in folder {extract_folder}.'
                                    f'Error : {e}'
                                    f'reverting to standard start and end date instead')

        write_log_entry(__file__, f'setting the time interval : {start_date} to {end_date}')

        # getting the extractors
        ex = extractors.get_extractors(download_folder, ca_subfolder,
                                       authentification_key=service_account_key, test_mode=testmode)

        # set expected columns
        headers = ['Date', 'Index', 'Description', 'Dépense', 'Numéro de référence',
                   'Recette', 'Compte', 'Catégorie',
                   'excluded']

        e: Extractor
        df: pd.DataFrame
        df_list = []
        # 1st step : iterate over the extractors
        write_log_section('Extract')
        for e in ex:
            write_log_section(f'Extractor : {e.name}')
            df = e.get_data()
            if df is None:
                write_log_entry(__file__, 'no extract found')
            else:
                write_log_entry(__file__, f'extract found, {len(df)} rows in it')
            # validate the content
            try:
                df = df[headers]
            except KeyError:
                raise KeyError(f'all the columns could not be found in the dataframe extracted by {e.name}')
            except TypeError:
                # no dataframe found
                pass
            # all good, we add the data
            if not df is None:
                df_list += [df]

        write_log_entry(__file__, f'{len(df_list)} dataframes loaded from the extractors')
        # 2nd step : merge and clean
        # merge
        write_log_section('Merge & Store')

        if len(df_list) > 0:
            write_log_entry(__file__, f'concatenating {len(df_list)} frames')
            global_df = c.concat_frames(df_list, headers)

            write_log_entry(__file__, 'adding extra columns')
            global_df = c.add_extra_columns(global_df)

            write_log_entry(__file__, f'filtering by date')
            global_df = c.filter_by_date(global_df, start_date, end_date)

            write_log_entry(__file__, f'setting excluded records')
            global_df = c.set_exclusion(global_df, exclusion_list)
            write_log_entry(__file__, f'records excluded : {len(global_df[global_df["excluded"] == True])} records')

            write_log_entry(__file__, f'formatting the Description')
            global_df['Description'] = c.format_description(global_df['Description'])

            write_log_entry(__file__, f'parsing the check number')
            global_df['Numéro de référence'] = c.parse_numero_cheque(global_df['Description'])

            write_log_entry(__file__, f'removing the zeroes')
            global_df = c.remove_zeroes('Dépense', global_df)
            global_df = c.remove_zeroes('Recette', global_df)

            write_log_entry(__file__, f'mapping to categories,using configured mapping file {mapping_file}')
            global_df = c.map_categories(global_df, mapcategories)

            write_log_entry(__file__, f'adding the current date as insertion date')
            global_df = c.add_insertdate(global_df, dt.date.today())

            # split the dataframe
            current, excluded, anterior = c.split_dataframes(global_df)
            write_log_entry(__file__, f'dataframes split, current rows : {len(current)}, '
                                      f'excluded rows : {len(excluded)}, '
                                      f'anterior rows : {len(anterior)}')

            # setting the index
            write_log_entry(__file__, f'setting the index at {start_index}')
            current = c.set_index('Index', start_index, current)

            s.store_frame(current, excluded, anterior,
                          ['Bureau', 'ca_extract.csv'], ['Bureau', 'ca_excluded.csv'], ['Bureau', 'ca_anterior.csv'])

            if not csv_only:
                if mode == 'ods':
                    # Writing to ODS
                    odscomptes = pyfin.indexfinder.get_latest_file(Path(comptes_folder))
                    if odscomptes is None:
                        raise TypeError(f'could not find a proper comptes file in {comptes_folder}')
                    s.store_frame_to_ods(current, odscomptes, 'Mouvements')
                if mode == 'sql':
                    # Writing to the database
                    s.store_frame_to_sql(current, finengine, 'comptes')
                if mode == 'sql2':
                    # Writing to the database with an improved mechanism
                    s.store_frame_to_sql_mode_7(current, finengine, start_date, end_date, start_index,
                                                simulate=simulate)

            write_log_entry(__file__, f'{len(current)} rows stored')
            # analysis
            write_log_entry(__file__, f'columns :')
            print(global_df.columns)
            write_log_entry(__file__, f'counts by date status')
            print(global_df.groupby('DateFilter')['Index'].count())
        else:
            write_log_entry(__file__, '0 rows to import')

        # archiving
        if archive:
            for e in ex:
                e.flush()
                write_log_entry(__file__, f'archiving the content of extractor {e.name}')


def import_mode_2(tablecomptes, intervaltype: str, intervalcount: int,
                  download_folder: str, ca_subfolder: str,
                  service_account_key: str,
                  testmode: bool, exclusion_list, mapping_file: str, mapcategoriesdf,
                  simulate: bool, archive: bool):
    write_log_section('launching new import mode')

    # Create an engine
    finengine = get_finance_engine()

    # Initializing the index
    try:
        start_index = pyfin.indexfinder.get_index_from_database(finengine, tablecomptes)
        write_log_entry(__file__, f'Index initialized to {start_index}')
    except Exception as e:
        write_log_entry(__file__, f'Could not find a proper index in the database'
                                  f'Error : {e}'
                                  f'Defaulting to 0 instead')
        start_index = 0

    # Initializing the indexes
    start_date, end_date = c.get_interval(interval_type=intervaltype, interval_count=intervalcount)
    write_log_entry(__file__, f'start and and date initialized to : {start_date}-{end_date}')

    # getting the last update dates
    last_updates = convert_last_updates_to_frame(get_last_updates_by_account())
    write_log_entry(__file__, f'last updates retrieved : {len(last_updates)} accounts')

    # getting the extractors
    ex = extractors.get_extractors(download_folder, ca_subfolder,
                                   authentification_key=service_account_key, test_mode=testmode)

    # set expected columns
    headers = ['Date', 'Index', 'Description', 'Dépense', 'Numéro de référence',
               'Recette', 'Compte', 'Catégorie',
               'excluded']

    e: Extractor
    df: pd.DataFrame
    # 1st step : iterate over the extractors
    for e in ex:
        write_log_section(f'Extractor : {e.name}')
        df = e.get_data()
        if df is None:
            write_log_entry(__file__, 'no extract found')
        else:
            write_log_entry(__file__, f'extract found, {len(df)} rows in it')
        # validate the content
        try:
            df = df[headers]
        except KeyError:
            raise KeyError(f'all the columns could not be found in the dataframe extracted by {e.name}')
        except TypeError:
            # no dataframe found
            pass
        # all good, we add the data
        if not df is None:
            try:
                last_update = last_updates.loc[e.name, 'LastUpdate']
                write_log_entry(__file__, f'last update date : {last_update}')
                start_date = last_update
            except KeyError:
                write_log_entry(__file__, 'could not find a last update date. Defaulting to start date instead...')

            write_log_entry(__file__, 'adding extra columns : économie, réglé, mois')
            df = c.add_extra_columns(df)

            write_log_entry(__file__, f'filtering by date')
            df = c.filter_by_date(df, start_date, end_date)

            write_log_entry(__file__, f'setting excluded records')
            df = c.set_exclusion(df, exclusion_list)
            write_log_entry(__file__, f'records excluded : {len(df[df["excluded"] == True])} records')

            write_log_entry(__file__, f'formatting the Description by switching to camelcase')
            df['Description'] = c.format_description(df['Description'])

            write_log_entry(__file__, f'parsing the check number')
            df['Numéro de référence'] = c.parse_numero_cheque(df['Description'])

            write_log_entry(__file__, f'removing the zeroes')
            df = c.remove_zeroes('Dépense', df)
            df = c.remove_zeroes('Recette', df)

            write_log_entry(__file__, f'mapping to keywords from map catégories table')
            df = c.map_keywords(df, 'Description', mapcategoriesdf['Keyword'].tolist())

            write_log_entry(__file__, f'enriching with the metadata from map_catégories table')
            df = c.map_extradata(df, 'Keyword', mapcategoriesdf)

            write_log_entry(__file__, f'adding the current date as insertion date')
            df = c.add_insertdate(df, dt.date.today())

            # split the dataframe
            current, excluded, anterior = c.split_dataframes(df)
            write_log_entry(__file__, f'dataframes split, current rows : {len(current)}, '
                                      f'excluded rows : {len(excluded)}, '
                                      f'anterior rows : {len(anterior)}')
            # setting the index
            write_log_entry(__file__, f'setting the index at {start_index}')
            current = c.set_index('Index', start_index, current)

            # Writing to the database with an improved mechanism
            s.store_frame_to_sql_mode_7(current, finengine, start_date, end_date, start_index,
                                        simulate=simulate, account_name=e.name)

            write_log_entry(__file__, f'{len(current)} rows stored')
            # analysis
            write_log_entry(__file__, f'columns :')
            print(df.columns)
            write_log_entry(__file__, f'counts by date status')
            print(df.groupby('DateFilter')['Index'].count())
            # increment
            start_index += len(current)
            # archiving
            if archive:
                e.flush()
                write_log_entry(__file__, f'archiving the content of extractor {e.name}')
        else:
            write_log_entry(__file__, '0 rows to import')


if __name__ == '__main__':
    main()
