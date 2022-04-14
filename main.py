import app.extract_ca as eca
import app.extract_cash as ecs
import app.extract_ba as ecb
import app.clean as c
import app.store as s

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('*** starting extraction... ***')
    # initialize data frame list
    df_list = []
    # extracting the Crédit Agricole
    print('* extracting Crédit Agricole *')
    df_ca = eca.extract_releve_ca('Téléchargements', 'ArchiveCA', True)
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
    df_cash = ecs.extract_cash_info()
    if df_cash is None:
        print('* no extract found *')
    else:
        print('* extract found *')
        df_cash = ecs.clean_cash_info(df_cash)
        print('* extract cleaned up *')
        df_list.append(df_cash)

    # extracting Boursorama
    print('* extracting Boursorama *')
    df_ba = ecb.extract_releve_ba('Téléchargements', 'ArchiveBA', True)
    if df_ba is None:
        print('* no extract found *')
    else:
        print('* extract found *')
        df_ba = ecb.clean_releve_ba(df_ba)
        print('* extract cleaned up *')
        df_list.append(df_ba)

    # merge
    print('*** merging all the data frames... ***')
    global_df = c.concat_frames(df_list)
    global_df = c.filter_by_date(global_df)

    print('*** storing the result... ***')
    s.store_frame(global_df, ['Bureau', 'ca_extract.csv'], ['Bureau', 'ca_excluded.csv'])
    #s.store_frame_to_ods(df_ca, ['Bureau', 'Comptes_2022.ods'], 'Import')
    print('*** extraction finished ***')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
