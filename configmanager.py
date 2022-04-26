from configparser import SafeConfigParser
from pathlib import Path


class AppConfiguration:
    __config_file__ = 'app.conf'

    def __init__(self):
        """ Initialize a config parser ; load it with all the properties ;
        then match with an existing config file ; and then save"""
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
        cf = Path(self.__config_file__)
        if cf.exists():
            existing_cp = SafeConfigParser()
            existing_cp.read(cf)
            for s in existing_cp.sections():
                for o in existing_cp.options(s):
                    self.__cp__[s][o] = existing_cp.get(s, o)

        # and then we dump the new and updated config file
        with open(self.__config_file__, 'w') as target:
            self.__cp__.write(target)

    @property
    def download_folder(self):
        return self.__cp__.get('SOURCE', 'DownloadFolder')

    @property
    def service_account_key(self):
        return self.__cp__.get('CREDENTIALS', 'ServiceAccountKey')

    @property
    def ca_subfolder(self):
        return self.__cp__.get('ARCHIVE', 'CreditAgricoleSubfolder')

    @property
    def ba_subfolder(self):
        return self.__cp__.get('ARCHIVE', 'BoursoramaSubfolder')

    @property
    def to_archive(self) -> bool:
        return self.__cp__.getboolean('ARCHIVE', 'Archive')
