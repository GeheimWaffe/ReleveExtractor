import sys
from pathlib import Path
import pyfin.extract_ca as eca
import pyfin.extract_cash as ecs
import pyfin.extract_ba as ecb
import pyfin.clean as c
import pyfin.store as s
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
             "--start-index : set the starting index"

    return result


def main(args=None, config_file: Path = None):
    """ main function to run the tool"""
    write_log_section('Starting the program')

    intervaltype = 'week'
    intervalcount = 1
    credentials = ''
    archive = True
    list_mappings = False
    start_index = 0

    # retrieving the args
    if args is None:
        args = sys.argv[1:]

    # print the args
    write_log_entry(__file__, f'arguments : {args}')

    for i in range(len(args)):
        if args[i] == '--interval-type':
            intervaltype = args[i+1]
        if args[i] == '--interval-count':
            intervalcount = int(args[i+1])
        if args[i] == '--set-credentials':
            credentials = args[i+1]
        if args[i] == '--list-mappings':
            list_mappings = True
        if args[i] == '--start-index':
            start_index = int(args[i+1])

    # set the options
    archive = not ("-no-archive" in args)

    # load config
    appconfig = AppConfiguration(config_file)

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
        for m in mps:
            print(m)
    else:

        # initialize data frame list
        df_list = []
        # extracting the Crédit Agricole
        write_log_section('Extract Crédit Agricole')
        df_ca = eca.extract_releve_ca(appconfig.download_folder, appconfig.ca_subfolder, appconfig.to_archive)

        if df_ca is None:
            write_log_entry(__file__, 'no extract found')
        else:
            write_log_entry(__file__, 'extract found')

            exclusion_list = ['REMBOURSEMENT DE PRET', 'ASSU. CNP PRET HABITAT', 'RETRAIT AU DISTRIBUTEUR']

            df_ca = eca.clean_releve_ca(df_ca, exclusion_list)
            write_log_entry(__file__, f'extract cleaned up, {len(df_ca)} rows found')
            df_list.append(df_ca)

        # extracting the cash
        write_log_section('Extract Liquide Vincent')
        if appconfig.service_account_key == '':
            write_log_entry(__name__, 'Warning : no authentication key found for logging to Google')
        df_cash = ecs.extract_cash_info(appconfig.service_account_key)
        if df_cash is None:
            write_log_entry(__file__, 'no extract found')
        else:
            write_log_entry(__file__, 'extract found')
            df_cash = ecs.clean_cash_info(df_cash)
            write_log_entry(__file__, f'extract cleaned up, {len(df_cash)} rows found')

            df_list.append(df_cash)

        # extracting Boursorama
        write_log_section('Extract Boursorama')
        df_ba = ecb.extract_releve_ba(appconfig.download_folder, appconfig.ba_subfolder, appconfig.to_archive)
        if df_ba is None:
            write_log_entry(__file__, 'no extract found')
        else:
            write_log_entry(__file__, 'extract found')
            df_ba = ecb.clean_releve_ba(df_ba)
            write_log_entry(__file__, f'extract cleaned up, {len(df_ba)} rows found')
            df_list.append(df_ba)

        # merge
        write_log_section('Merge & Store')

        # set targeted headers
        headers = ['Date', 'Index', 'Description', 'Dépense', 'N° de référence',
                               'Recette', 'Taux de remboursement', 'Compte', 'Catégorie', 'Economie', 'Réglé', 'Mois', 'excluded']

        if len(df_list) > 0:
            write_log_entry(__file__, 'adding extra columns')
            df_list = c.add_extra_columns(df_list)

            write_log_entry(__file__, f'concatenating {len(df_list)} frames')
            global_df = c.concat_frames(df_list, headers)

            write_log_entry(__file__, f'filtering by date')
            global_df = c.filter_by_date(global_df, intervaltype, intervalcount)

            write_log_entry(__file__, f'parsing the check number')
            global_df['N° de référence'] = c.parse_numero_cheque(global_df['Description'])

            write_log_entry(__file__, f'setting the index ')
            global_df['Index'] = range(start_index, start_index + len(global_df))

            write_log_entry(__file__, f'mapping to categories,using configured mapping file {appconfig.mapping_file}')
            global_df = c.map_categories(global_df, appconfig.mapping_file)

            s.store_frame(global_df, ['Bureau', 'ca_extract.csv'], ['Bureau', 'ca_excluded.csv'])

            # s.store_frame_to_ods(global_df, ['Bureau', 'Comptes_2022.ods'], 'Mouvements')
            write_log_entry(__file__, f'{len(global_df)} rows stored')
        else:
            write_log_entry(__file__, '0 rows to import')

