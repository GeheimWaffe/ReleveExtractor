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
from pyfin.configmanager import CategoryMapper


def get_help() -> str:
    result = "Relevé Extractor (c). Following parameters are possible : \n" \
             "-help : print this help" \
             "-no-archive : the files will not be archived in sub-folders\n" \
             "" \
             "following configurations can be made :" \
             "--interval-type [day|week|month] : sets the interval of data, default : week" \
             "--interval-count [number] : sets the number of intervals, default : 1" \
             "--set-credentials [path] : configures the credentials for accessing google drive" \
             "--list-mappings : list the currently configured mappings" \
             "--no-archive : do not archive the input files" \
             "--get-index: gets the latest index" \
             "--csv-only: exports only to csv"
    return result


def main(args=None, config_file: Path = None):
    """ main function to run the tool"""
    write_log_section('Starting the program')

    intervaltype = 'week'
    intervalcount = 1
    credentials = ''
    list_mappings = False
    get_index = False
    csv_only = False
    threshold = 0
    testmode = False

    # retrieving the args
    if args is None:
        args = sys.argv[1:]

    # print the args
    write_log_entry(__file__, f'arguments : {args}')

    for i in range(len(args)):
        if args[i] == '--interval-type':
            intervaltype = args[i + 1]
        if args[i] == '--interval-count':
            intervalcount = int(args[i + 1])
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
        if args[i] == '--help':
            print(get_help())
            return

    # set the options
    archive = not ("--no-archive" in args)

    # load config
    appconfig = AppConfiguration(config_file)

    # load category mappings
    catmap = CategoryMapper()
    write_log_entry(__file__, f'loading the category mappings at {appconfig.mapping_file}')
    catmap.load_csv(Path().home().joinpath(appconfig.mapping_file))
    write_log_entry(__file__, f'category mappings loaded : {len(catmap.get_mappins())} found')

    # set the exclusion list
    exclusion_list = appconfig.excluded_keywords

    # set the threshold
    if threshold <= 0:
        threshold = appconfig.periodization_threshold

    # fix the credentials
    if credentials != '':
        appconfig.service_account_key = credentials

    write_log_section('Defining the parameters')
    write_log_entry(__file__, f'archive the data : {archive}')
    write_log_entry(__file__, f' interval type : {intervaltype}')
    write_log_entry(__file__, f' interval count : {intervalcount}')
    write_log_entry(__file__, f' google drive location : {appconfig.service_account_key}')
    write_log_entry(__file__, f'periodization threshold: {appconfig.periodization_threshold}')

    if list_mappings:
        catmap = CategoryMapper()
        catmap.load_csv(Path.home().joinpath(appconfig.mapping_file))
        write_log_entry(__file__, f'list of mappings from file {appconfig.mapping_file}')
        mps = catmap.get_mappins()
        print(mps)
    elif get_index:
        lastcompte = pyfin.indexfinder.get_latest_file(Path(appconfig.extract_folder))
        last_index = pyfin.indexfinder.get_index_from_file(lastcompte)
        write_log_entry(__file__, f'the last index is : {last_index}')
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
            try:
                start_index = pyfin.indexfinder.get_index_from_file(lastcompte)
                start_index += 1
                write_log_entry(__file__, f'Index initialized to {start_index}')
            except Exception as e:
                write_log_entry(__file__, f'Could not find a proper index source in the file  {lastcompte.name}.'
                                          f'Error : {e}'
                                          f'Defaulting to 0 instead')
                start_index = 0
            try:
                start_date = pyfin.indexfinder.get_lastdate_from_file(lastcompte)
                start_date += dt.timedelta(days=1)
                write_log_entry(__file__, f'Start date adjusted to {start_date}')
            except Exception as e:
                write_log_entry(__file__, f'Could not find a proper start date in folder {appconfig.extract_folder}.'
                                          f'Error : {e}'
                                          f'reverting to standard start and end date instead')

        write_log_entry(__file__, f'setting the time interval : {start_date} to {end_date}')

        # getting the extractors
        ex = extractors.get_extractors(appconfig.download_folder, appconfig.ca_subfolder,
                                       authentification_key=appconfig.service_account_key)

        # set expected columns
        headers = ['Date', 'Index', 'Description', 'Dépense', 'N° de référence',
                   'Recette', 'Taux de remboursement', 'Compte', 'Catégorie',
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
            global_df['N° de référence'] = c.parse_numero_cheque(global_df['Description'])

            write_log_entry(__file__, f'removing the zeroes')
            global_df = c.remove_zeroes('Dépense', global_df)
            global_df = c.remove_zeroes('Recette', global_df)

            write_log_entry(__file__, f'mapping to categories,using configured mapping file {appconfig.mapping_file}')
            global_df = c.map_categories(global_df, catmap.get_mappins())

            write_log_entry(__file__, f'adding the current date as insertion date')
            global_df = c.add_insertdate(global_df, dt.date.today())

            # split the dataframe
            current, excluded, anterior = c.split_dataframes(global_df)
            write_log_entry(__file__, f'dataframes split, current rows : {len(current)}, '
                                      f'excluded rows : {len(excluded)}, '
                                      f'anterior rows : {len(anterior)}')

            # optional : spread some rows over the full year
            desc = c.get_transaction_description(current)
            indexes = {'Dépense': [], 'Recette': []}
            # reset the index to avoid duplicates
            current = current.reset_index(drop=True)
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
                odscomptes = pyfin.indexfinder.get_latest_file(Path(appconfig.comptes_folder))
                if odscomptes is None:
                    raise TypeError(f'could not find a proper comptes file in {appconfig.comptes_folder}')
                s.store_frame_to_ods(current, odscomptes, 'Mouvements')

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
