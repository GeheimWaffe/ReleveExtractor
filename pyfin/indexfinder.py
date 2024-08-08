""" Ce package permet de récupérer un numéro d'index.
La logique est la suivante :
- récupérer le dernier extract des comptes bancaires à la racine du répertoire des comptes (paramètre)
- charger le contenu sous forme de dataframe
- prendre le maximum de la colonne d'index
"""
import pandas as pd
from pathlib import Path