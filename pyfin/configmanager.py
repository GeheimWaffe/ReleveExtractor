from configparser import ConfigParser
from pathlib import Path
import csv


class AppConfiguration:
    __config_file_name__ = 'pyfin.conf'

    def get_filepath(self) -> Path:
        return Path.home().joinpath(self.__config_file_name__)

    def __init__(self, config_file: Path = None):
        """ Initialize a config parser ; load it with all the properties ;
        then match with an existing config file ; and then save

        :param config_file: if a custom config file is a provided as a Path"""
        self.__cp__ = ConfigParser()

        # configuring the default config file
        self.__cp__.add_section('CREDENTIALS')
        self.__cp__['CREDENTIALS']['ServiceAccountKey'] = ''

        self.__cp__.add_section('SOURCE')
        self.__cp__['SOURCE']['DownloadFolder'] = 'Téléchargements'

        self.__cp__.add_section('ARCHIVE')
        self.__cp__['ARCHIVE']['CreditAgricoleSubfolder'] = 'ArchiveCA'
        self.__cp__['ARCHIVE']['BoursoramaSubfolder'] = 'ArchiveBA'
        self.__cp__['ARCHIVE']['Archive'] = 'True'

        self.__cp__.add_section('MAPPINGS')
        self.__cp__['MAPPINGS']['MappingFile'] = 'pyfin_loader_category_mappings.csv'

        self.__cp__.add_section('EXTRACTS')
        self.__cp__['EXTRACTS']['ExtractFolder'] = '/home/vincent/Extracts'

        self.__cp__.add_section('COMPTES')
        self.__cp__['COMPTES']['ComptesFolder'] = '/home/vincent/Comptes'

        self.__cp__.add_section('EXCLUSIONS')
        self.__cp__['EXCLUSIONS']['keywords'] = 'REMBOURSEMENT DE PRET, ASSU. CNP PRET HABITAT, RETRAIT AU DISTRIBUTEUR'

        self.__cp__.add_section('OTHER')
        self.__cp__['OTHER']['periodization_threshold'] = '150'

        # now loading the existing config if any
        if config_file is None:
            cf = self.get_filepath()
        else:
            cf = config_file

        if cf.exists():
            existing_cp = ConfigParser()
            existing_cp.read(cf)
            for s in existing_cp.sections():
                for o in existing_cp.options(s):
                    self.__cp__[s][o] = existing_cp.get(s, o)

        # and then we dump the new and updated config file
        with open(cf, 'w') as target:
            self.__cp__.write(target)

    @property
    def download_folder(self):
        return self.__cp__.get('SOURCE', 'DownloadFolder')

    @property
    def service_account_key(self):
        return self.__cp__.get('CREDENTIALS', 'ServiceAccountKey')

    @service_account_key.setter
    def service_account_key(self, value: str):
        self.__cp__['CREDENTIALS']['ServiceAccountKey'] = value

    @property
    def ca_subfolder(self):
        return self.__cp__.get('ARCHIVE', 'CreditAgricoleSubfolder')

    @property
    def ba_subfolder(self):
        return self.__cp__.get('ARCHIVE', 'BoursoramaSubfolder')

    @property
    def to_archive(self) -> bool:
        return self.__cp__.getboolean('ARCHIVE', 'Archive')

    @property
    def mapping_file(self) -> str:
        return self.__cp__.get('MAPPINGS', 'MappingFile')

    @property
    def extract_folder(self) -> str:
        return self.__cp__.get('EXTRACTS', 'ExtractFolder')

    @property
    def comptes_folder(self) -> str:
        return self.__cp__.get('COMPTES', 'ComptesFolder')

    @property
    def excluded_keywords(self) -> list:
        strlist = self.__cp__.get('EXCLUSIONS', 'keywords')
        result = [c.strip() for c in strlist.split(',')]
        return result

    @property
    def periodization_threshold(self) -> float:
        return float(self.__cp__.get('OTHER', 'periodization_threshold'))


class CategoryMapper:
    """ class used for mapping categories """
    __mappings__ = []

    def load_csv(self, csv_path: Path) -> str:
        if csv_path.exists() and csv_path.is_file() and csv_path.suffix == '.csv':
            with open(csv_path, newline='') as csv_file:
                reader = csv.reader(csv_file, delimiter=',', quotechar='"')
                for line in reader:
                    self.__mappings__.append(line)

            return f'round rows : {self.__mappings__}'
        else:
            return f'no file found for path, path incorrect, or wrong stem : {csv_path}'

    def get_mappins(self) -> list:
        """ return the mappings
        :return a set of (mask, category) maps"""
        return self.__mappings__
