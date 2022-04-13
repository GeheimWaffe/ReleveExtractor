import app.extract_ca as eca
import app.extract_cash as ecs
import app.store as s

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('*** starting extraction... ***')
    # extracting the Crédit Agricole
    df_ca = eca.extract_releve_ca('Téléchargements', 'ArchiveCA', False)
    if df_ca is None:
        print('*** no extract found for Crédit Agricole ***')
    else:
        exclusion_list = ['REMBOURSEMENT DE PRET', 'ASSU. CNP PRET HABITAT', 'RETRAIT AU DISTRIBUTEUR']

        df_ca = eca.clean_releve_ca(df_ca, exclusion_list)
    # extracting the cash
    df_cash = ecs.extract_cash_info()
    if df_cash is None:
        print('*** no Google sheets file found for the cash')
    else:
        df_cash = ecs.clean_cash_info(df_cash)

    s.store_frame([df_ca, df_cash], ['Bureau', 'ca_extract.csv'], ['Bureau', 'ca_excluded.csv'])
    #s.store_frame_to_ods(df_ca, ['Bureau', 'Comptes_2022.ods'], 'Import')
    print('*** extraction finished ***')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
