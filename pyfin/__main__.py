import sys
import pyfin.extract_ca as eca
import pyfin.extract_cash as ecs
import pyfin.extract_ba as ecb
import pyfin.clean as c
import pyfin.store as s
from pyfin.configmanager import AppConfiguration

def get_help() -> str:
    result = "Relevé Extractor (c). Following parameters are possible : \n" \
             "-help : print this help" \
             "-no-archive : the files will not be archived in sub-folders\n" \
             "" \
             "following configurations can be made :" \
             "--interval-type [day|week|month] : sets the interval of data, default : week" \
             "--interval-count [number] : sets the number of intervals, default : 1" \
             "--set-credentials [path] : configures the credentials for accessing google drive"

    return result

def main(args=None):
    """ main function to run the tool"""
    intervaltype = 'week'
    intervalcount = 1
    credentials = ''

    # checking if we are in prod or test
    if not args is None:
        for i in range(len(args)):
            if args[i] == '--interval-type':
                intervaltype = args[i+1]
            if args[i] == '--interval-count':
                intervalcount = args[i+1]
            if args[i] == '--set-credentials':
                credentials = args[i+1]

        # set the options
        archive = not ("-no-archive" in args)

    # load config
    appconfig = AppConfiguration()
    # fix the credentials
    if credentials != '':
        appconfig.service_account_key = credentials

    print('*** parameters : ***')
    print(f' archive the data : {archive}')
    print(f' interval type : {intervaltype}')
    print(f' interval count : {intervalcount}')
    print(f' google drive location : {appconfig.service_account_key}')

    print('*** starting extraction... ***')
    # initialize data frame list
    df_list = []
    # extracting the Crédit Agricole
    print('* extracting Crédit Agricole *')
    df_ca = eca.extract_releve_ca(appconfig.download_folder, appconfig.ca_subfolder, appconfig.to_archive)

    if df_ca is None:
        print('* no extract found *')
    else:
        print('* extract found *')
        exclusion_list = ['REMBOURSEMENT DE PRET', 'ASSU. CNP PRET HABITAT', 'RETRAIT AU DISTRIBUTEUR']

        df_ca = eca.clean_releve_ca(df_ca, exclusion_list)
        print('* extract cleaned up *')
        df_list.append(df_ca)

    # extracting the cash
    print('* extracting Liquide Vincent *')
    df_cash = ecs.extract_cash_info(appconfig.service_account_key)
    if df_cash is None:
        print('* no extract found *')
    else:
        print('* extract found *')
        df_cash = ecs.clean_cash_info(df_cash)
        print('* extract cleaned up *')
        df_list.append(df_cash)

    # extracting Boursorama
    print('* extracting Boursorama *')
    df_ba = ecb.extract_releve_ba(appconfig.download_folder, appconfig.ba_subfolder, appconfig.to_archive)
    if df_ba is None:
        print('* no extract found *')
    else:
        print('* extract found *')
        df_ba = ecb.clean_releve_ba(df_ba)
        print('* extract cleaned up *')
        df_list.append(df_ba)

    # merge
    if len(df_list) > 0:
        print('*** merging all the data frames... ***')
        global_df = c.concat_frames(df_list)
        global_df = c.filter_by_date(global_df, intervaltype, intervalcount)

        print('*** storing the result... ***')
        s.store_frame(global_df, ['Bureau', 'ca_extract.csv'], ['Bureau', 'ca_excluded.csv'])
        # s.store_frame_to_ods(global_df, ['Bureau', 'Comptes_2022.ods'], 'Mouvements')
        print('*** extraction finished ***')
    else:
        print('*** no data found ***')


if __name__ == '__main__':
    main(sys.argv[1:])