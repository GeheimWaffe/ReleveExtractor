from configparser import SafeConfigParser
from pathlib import Path


class AppConfiguration:
    __config_file_name__ = 'pyfin.conf'

    def get_filepath(self) -> Path:
        return Path.home().joinpath(self.__config_file_name__)

    def __init__(self, config_file: Path = None):
        """ Initialize a config parser ; load it with all the properties ;
        then match with an existing config file ; and then save

        :param config_file: if a custom config file is a provided as a Path"""
        self.__cp__ = SafeConfigParser()

        # configuring the default config file
        self.__cp__.add_section('CREDENTIALS')
        self.__cp__['CREDENTIALS']['ServiceAccountKey'] = ''

        self.__cp__.add_section('SOURCE')
        self.__cp__['SOURCE']['DownloadFolder'] = 'Téléchargements'

        self.__cp__.add_section('ARCHIVE')
        self.__cp__['ARCHIVE']['CreditAgricoleSubfolder'] = 'ArchiveCA'
        self.__cp__['ARCHIVE']['BoursoramaSubfolder'] = 'ArchiveBA'
        self.__cp__['ARCHIVE']['Archive'] = 'True'

        # now loading the existing config if any
        if config_file is None:
            cf = self.get_filepath()
        else:
            cf = config_file

        if cf.exists():
            existing_cp = SafeConfigParser()
            existing_cp.read(cf)
            for s in existing_cp.sections():
                for o in existing_cp.options(s):
                    self.__cp__[s][o] = existing_cp.get(s, o)

        # and then we dump the new and updated config file
        with open(self.get_filepath(), 'w') as target:
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
