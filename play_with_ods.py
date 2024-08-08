from pyfin.odfpandas import SpreadsheetWrapper
from pathlib import Path

p = Path().home().joinpath('Documents', 'TestWorkbook.ods')

sh = SpreadsheetWrapper()
sh.load(p)
sw = sh.get_sheets()['Feuille1']

# note : pour exécuter dans la console, utiliser runfile('play_with_ods.py')