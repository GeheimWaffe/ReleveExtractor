import app.extract as e
import app.clean as c
import app.store as s
import datetime as dt

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('*** starting extraction... ***')
    df = e.extract_raw_releve('Téléchargements', 'ArchiveCA', False)
    if df is None:
        print('*** extraction aborted, no file found ***')
    else:
        exclusion_list = ['REMBOURSEMENT DE PRET', 'ASSU. CNP PRET HABITAT', 'RETRAIT AU DISTRIBUTEUR']

        df = c.prepare_dataframe(df, exclusion_list)
        s.store_frame(df, ['Bureau', 'ca_extract.csv'], ['Bureau', 'ca_excluded.csv'])
        print('*** extraction finished ***')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
