import sys
from pathlib import Path

import pandas as pd

from pyfin.coremodel import Extractor, explode_values
import datetime as dt
import pyfin.extractors as extractors
import pyfin.coremodel as c
import pyfin.store as s
import pyfin.indexfinder
from pyfin.logger import write_log_entry
from pyfin.logger import write_log_section
from pyfin.configmanager import AppConfiguration
from pyfin.database import get_finance_engine
from pyfin.database import get_map_organismes, get_map_categories


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
             "--simulate : only simulates the loading, without commit"

    return result


def main(args=None, config_file: Path = None):
    """ main function to run the tool"""
    write_log_section('Starting the program')

    intervaltype = 'week'
    intervalcount = 1
    interval_manual_mode = False
    credentials = ''
    list_mappings = False
    get_index = False
    csv_only = False
    threshold = 0
    testmode = False
    mode = 'none'
    simulate = False

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
        if args[i] == '--list-mappings':
            list_mappings = True
        if args[i] == '--get-index':
            get_index = True
        if args[i] == '--csv-only':
            csv_only = True
        if args[i] == '--set-threshold':
            threshold = float(args[i + 1])
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
        if args[i] == '--help':
            print(get_help())
            return

    if mode == 'none':
        raise ValueError('No mode was selected (ODS, or SQL)')

    # set the options
    archive = not ("--no-archive" in args) or simulate

    # load config
    appconfig = AppConfiguration(config_file)

    # load category mappings
    write_log_entry(__file__, f'loading the category mappings from the database')
    mapcategories = get_map_categories()
    write_log_entry(__file__, f'category mappings loaded : {len(mapcategories)} found')

    # load the organisme mappings
    write_log_entry(__file__, f'loading the organisme mappings')
    maporganismes = get_map_organismes()
    write_log_entry(__file__, f'{len(maporganismes)} organismes found')

    # set the exclusion list
    # Deactivate the exclusion mechanism
    # exclusion_list = appconfig.excluded_keywords
    exclusion_list = []

    # set the threshold
    if threshold <= 0:
        threshold = appconfig.periodization_threshold

    # fix the credentials
    if credentials != '':
        appconfig.service_account_key = credentials

    # Create an engine
    finengine = get_finance_engine()

    write_log_section('Defining the parameters')
    write_log_entry(__file__, f'archive the data : {archive}')
    write_log_entry(__file__, f' interval type : {intervaltype}')
    write_log_entry(__file__, f' interval count : {intervalcount}')
    write_log_entry(__file__, f' google drive location : {appconfig.service_account_key}')
    write_log_entry(__file__, f'periodization threshold: {appconfig.periodization_threshold}')
    write_log_entry(__file__, f'mode : {mode}')

    if list_mappings:
        for m in mapcategories:
            print(m)
    elif get_index:
        # Check the proper mode
        if mode == 'ods':
            lastcompte = pyfin.indexfinder.get_latest_file(Path(appconfig.extract_folder))
            last_index = pyfin.indexfinder.get_index_from_file(lastcompte)
            write_log_entry(__file__, f'the last index from the file is : {last_index}')
        elif mode == 'sql':
            last_index = pyfin.indexfinder.get_index_from_database(finengine, appconfig.tablecomptes)
            write_log_entry(__file__, f'the last index from the database is : {last_index}')
    else:
        # Calculate the last index and start, end dates
        write_log_section('Connecting to the previous exports')
        lastcompte = pyfin.indexfinder.get_latest_file(Path(appconfig.extract_folder))
        # Initialize the values
        start_date, end_date = c.get_interval(interval_type=intervaltype, interval_count=intervalcount)
        start_index = 0
        write_log_entry(__file__, f'start and and date initialized to : {start_date}-{end_date}')
        if lastcompte is None:
            write_log_entry(__file__, f'could not find a proper file in {appconfig.extract_folder}')
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
                    start_index = pyfin.indexfinder.get_index_from_database(finengine, appconfig.tablecomptes)
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
                                    f'Could not find a proper start date in folder {appconfig.extract_folder}.'
                                    f'Error : {e}'
                                    f'reverting to standard start and end date instead')
            if mode == 'sql' or mode == 'sql2':
                try:
                    if not interval_manual_mode:
                        start_date = pyfin.indexfinder.get_lastdate_from_database(finengine, appconfig.tablecomptes)
                        # start_date += dt.timedelta(days=1)
                        write_log_entry(__file__, f'Start date adjusted to {start_date}')
                        # TODO : appeler la session pour récupérer la table des dernières mises à jour
                    else:
                        pass
                except Exception as e:
                    write_log_entry(__file__,
                                    f'Could not find a proper start date in folder {appconfig.extract_folder}.'
                                    f'Error : {e}'
                                    f'reverting to standard start and end date instead')

        write_log_entry(__file__, f'setting the time interval : {start_date} to {end_date}')

        # getting the extractors
        ex = extractors.get_extractors(appconfig.download_folder, appconfig.ca_subfolder,
                                       authentification_key=appconfig.service_account_key, test_mode=testmode)

        # set expected columns
        # TODO : remove the excluded column
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
                # TODO retrieve the last update date ; set it as a column
                df_list += [df]

        # TODO : add the Date Out of Bound label to the headers, add the previous insert date in the headers

        write_log_entry(__file__, f'{len(df_list)} dataframes loaded from the extractors')
        # 2nd step : merge and clean
        # merge
        write_log_section('Merge & Store')

        if len(df_list) > 0:
            write_log_entry(__file__, f'concatenating {len(df_list)} frames')
            global_df = c.concat_frames(df_list, headers)

            write_log_entry(__file__, 'adding extra columns')
            global_df = c.add_extra_columns(global_df)

            # TODO : remonter le filtrage vers le haut, au moment de la récupération du dataframe
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

            write_log_entry(__file__, f'mapping to categories,using configured mapping file {appconfig.mapping_file}')
            global_df = c.map_categories(global_df, mapcategories)

            write_log_entry(__file__, f'mapping the organismes, using the database')
            global_df = c.map_organismes(global_df, maporganismes)

            write_log_entry(__file__, f'adding the current date as insertion date')
            global_df = c.add_insertdate(global_df, dt.date.today())

            # split the dataframe
            current, excluded, anterior = c.split_dataframes(global_df)
            write_log_entry(__file__, f'dataframes split, current rows : {len(current)}, '
                                      f'excluded rows : {len(excluded)}, '
                                      f'anterior rows : {len(anterior)}')

            # optional : spread some rows over the full year
            indexes = {'Dépense': [], 'Recette': []}
            # reset the index to avoid duplicates
            current = current.reset_index(drop=True)
            desc = c.get_transaction_description(current)
            for valuecolumn in indexes.keys():
                for i in current.index.values:
                    value = current.loc[i, valuecolumn]
                    periodize = ''
                    if value is None:
                        pass
                    else:
                        if value > threshold:
                            while periodize not in ['y', 'n']:
                                periodize = input(
                                    f'{desc.loc[i]} : {valuecolumn} of {str(value)} above threshold. Periodize over year (y/n) ?')
                                if periodize == 'y':
                                    indexes[valuecolumn] += [i]
            # explode
            for valuecolumn in indexes.keys():
                if len(indexes[valuecolumn]) > 0:
                    write_log_entry(__file__, f'exploding values for indexes : {indexes[valuecolumn]}')
                    current = explode_values(current, valuecolumn, 'Mois', indexes[valuecolumn])

            # setting the index
            write_log_entry(__file__, f'setting the index at {start_index}')
            current = c.set_index('Index', start_index, current)

            s.store_frame(current, excluded, anterior,
                          ['Bureau', 'ca_extract.csv'], ['Bureau', 'ca_excluded.csv'], ['Bureau', 'ca_anterior.csv'])

            if not csv_only:
                if mode == 'ods':
                    # Writing to ODS
                    odscomptes = pyfin.indexfinder.get_latest_file(Path(appconfig.comptes_folder))
                    if odscomptes is None:
                        raise TypeError(f'could not find a proper comptes file in {appconfig.comptes_folder}')
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


if __name__ == '__main__':
    main()
