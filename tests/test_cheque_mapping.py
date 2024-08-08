from unittest import TestCase
from pyfin.clean import extract_numero_cheque, parse_numero_cheque
from pandas import Series

class TestChequeMapper(TestCase):
    def test_extract_numero_cheque(self):
        label = 'Cheque Emis 9355334'
        result = extract_numero_cheque(label)
        self.assertEqual('9355334', result, 'Extraction du numéro de chèque a échoué')

    def test_parse_numero_cheque(self):
        s = Series(['Cheque Emis 1234567', 'Cheque Emis 7654321', 'Courses Leclerc'])
        result = parse_numero_cheque(s)
        self.assertEqual(result[0], '1234567', 'Vérification de la longueur de la série')
        self.assertEqual(result[1], '7654321', 'Vérification de la longueur de la série')

