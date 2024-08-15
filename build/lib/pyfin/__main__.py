import sys
from pathlib import Path

import pandas as pd

from pyfin.coremodel import Extractor
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
             "--get-index: gets the latest index"
    return result


def main(args=None, config_file: Path = None):
    """ main function to run the tool"""
    write_log_section('Starting the program')

    intervaltype = 'week'
    intervalcount = 1
    credentials = ''
    list_mappings = False
    get_index = False

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
        if args[i] == '--help':
            print(get_help())
            return

    # set the options
    archive = not ("--no-archive" in args)

    # load config
    appconfig = AppConfiguration(config_file)

    # load category mappings
    catmap = CategoryMapper()
    write_log_entry(__file__, 'loading the category mappings')
    catmap.load_csv(Path(appconfig.mapping_file))
    write_log_entry(__file__, f'category mappings loaded : {len(catmap.get_mappins())} found')

    # set the exclusion list
    exclusion_list = ['REMBOURSEMENT DE PRET', 'ASSU. CNP PRET HABITAT', 'RETRAIT AU DISTRIBUTEUR']

    # fix the credentials
    if credentials != '':
        appconfig.service_account_key = credentials

    write_log_section('Defining the parameters')
    write_log_entry(__file__, f'archive the data : {archive}')
    write_log_entry(__file__, f' interval type : {intervaltype}')
    write_log_entry(__file__, f' interval count : {intervalcount}')
    write_log_entry(__file__, f' google drive location : {appconfig.service_account_key}')

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
            # all good, we add the data
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

            write_log_entry(__file__, f'setting the index at {start_index}')
            global_df = c.set_index('Index', start_index, global_df)

            write_log_entry(__file__, f'removing the zeroes')
            global_df = c.remove_zeroes('Dépense', global_df)
            global_df = c.remove_zeroes('Recette', global_df)

            write_log_entry(__file__, f'mapping to categories,using configured mapping file {appconfig.mapping_file}')
            global_df = c.map_categories(global_df, catmap.get_mappins())

            write_log_entry(__file__, f'adding the current date as insertion date')
            global_df = c.add_insertdate(global_df, dt.date.today())

            s.store_frame(global_df,
                          ['Bureau', 'ca_extract.csv'], ['Bureau', 'ca_excluded.csv'], ['Bureau', 'ca_anterior.csv'])

            # s.store_frame_to_ods(global_df, ['Bureau', 'Comptes_2022.ods'], 'Mouvements')
            write_log_entry(__file__, f'{len(global_df)} rows stored')
            # analysis
            write_log_entry(__file__, f'columns :')
            print(global_df.columns)
            write_log_entry(__file__, f'counts by date status')
            print(global_df.groupby('DateFilter')['Index'].count())
        else:
            write_log_entry(__file__, '0 rows to import')


if __name__ == '__main__':
    main()
