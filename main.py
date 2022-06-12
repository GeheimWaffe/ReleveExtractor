from pathlib import Path
from pyfin.__main__ import main

# define the custom configuration file
dev_conf_file = Path('app.conf')
main(None, dev_conf_file)