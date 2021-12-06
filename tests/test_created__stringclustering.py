import datetime
import numpy as np
from optimus.tests.base import TestBase
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort, df_dicts_equal, results_equal


def Timestamp(t):
    return datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")


NaT = np.datetime64('NaT')
nan = float("nan")
inf = float("inf")


class TestStringclusteringPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None

    def test_cols_double_metaphone_all(self):
        df = self.df.copy()
        result = df.cols.double_metaphone(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): [('NN', ''), ('NN', ''), ('NN', ''), ('NN', ''), ('NN', ''), ('NN', '')], ('date arrival', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', '')], ('height(ft)', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('NN', ''), ('', '')], ('last date seen', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', '')], ('last position seen', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('NN', ''), ('NN', '')], ('rank', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', '')], ('Cybertronian', 'object'): [('TR', ''), ('TR', ''), ('TR', ''), ('TR', ''), ('TR', ''), ('FLS', '')], ('Date Type', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', '')], ('age', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', '')], ('function', 'object'): [('LTR', ''), ('ASPNJ', 'ASPNK'), ('SKRT', ''), ('FRSTLTNNT', ''), ('NN', ''), ('PTLSTXN', '')], ('names', 'object'): [('APTMS', ''), ('PMPLLP', ''), ('MTRPLKS', ''), ('PMPLP', ''), ('MTRPPLKS', ''), ('MTRPLKSKSKSKSKS', '')], ('timestamp', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', '')], ('weight(t)', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('NN', '')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_double_metaphone_multiple(self):
        df = self.df.copy()
        result = df.cols.double_metaphone(cols=['NullType', 'Cybertronian', 'timestamp'], output_cols=['nt', 'ct', 'ts'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): [('NN', ''), ('NN', ''), ('NN', ''), ('NN', ''), ('NN', ''), ('NN', '')], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('ct', 'object'): [('TR', ''), ('TR', ''), ('TR', ''), ('TR', ''), ('TR', ''), ('FLS', '')], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('ts', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', '')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_double_metaphone_numeric(self):
        df = self.df.copy()
        result = df.cols.double_metaphone(cols=['rank'], output_cols=['rk'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('rk', 'object'): [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', '')], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_double_metaphone_string(self):
        df = self.df.copy()
        result = df.cols.double_metaphone(cols=['names'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): [('APTMS', ''), ('PMPLLP', ''), ('MTRPLKS', ''), ('PMPLP', ''), ('MTRPPLKS', ''), ('MTRPLKSKSKSKSKS', '')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_fingerprint_all(self):
        df = self.df.copy()
        result = df.cols.fingerprint(cols='*')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_fingerprint_multiple(self):
        df = self.df.copy()
        result = df.cols.fingerprint(cols=['NullType', 'Cybertronian', 'timestamp'], output_cols=['nt', 'ct', 'ts'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_fingerprint_numeric(self):
        df = self.df.copy()
        result = df.cols.fingerprint(cols=['rank'], output_cols=['rk'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_fingerprint_string(self):
        df = self.df.copy()
        result = df.cols.fingerprint(cols=['names'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_levenshtein_all_col(self):
        df = self.df.copy()
        result = df.cols.levenshtein(cols='*', other_cols=['date arrival', 'weight(t)', 'age', 'height(ft)', 'japanese name', 'rank', 'last date seen', 'names', 'last position seen', 'Cybertronian', 'NullType', 'Date Type', 'function(binary)', 'function', 'timestamp'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_levenshtein_all_value(self):
        df = self.df.copy()
        result = df.cols.levenshtein(cols='*', value=['1a#-s', 'ERR', 'd2e', '0', '[]', "''", '', '1', 'lu', '2016', '5000000', 'aeiou', 'abc#&^', '2014-06-23', 'nan.'])
        expected = self.create_dataframe(data={('NullType', 'int64'): [5, 5, 5, 5, 5, 5], ('date arrival', 'int64'): [10, 10, 10, 10, 10, 10], ('height(ft)', 'int64'): [4, 4, 4, 4, 3, 5], ('last date seen', 'int64'): [9, 9, 9, 9, 9, 9], ('last position seen', 'int64'): [20, 20, 21, 21, 4, 4], ('rank', 'int64'): [2, 2, 2, 2, 2, 2], ('Cybertronian', 'int64'): [4, 4, 4, 4, 4, 5], ('Date Type', 'int64'): [9, 9, 9, 9, 9, 9], ('age', 'int64'): [7, 7, 7, 7, 7, 7], ('function', 'int64'): [6, 9, 8, 16, 4, 14], ('names', 'int64'): [7, 12, 9, 9, 11, 13], ('timestamp', 'int64'): [10, 10, 10, 10, 10, 10], ('weight(t)', 'int64'): [6, 6, 6, 6, 6, 6]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_levenshtein_multiple_col(self):
        df = self.df.copy()
        result = df.cols.levenshtein(cols=['NullType', 'Cybertronian', 'timestamp'], other_cols=['height(ft)', 'function', 'Date Type'], output_cols=['nt-ht', 'ct-ft', 'ts-dt'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ("NullType_['nt-ht', 'ct-ft', 'ts-dt']", 'int64'): [5, 4, 4, 4, 3, 5], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ("Cybertronian_['nt-ht', 'ct-ft', 'ts-dt']", 'int64'): [5, 8, 7, 13, 3, 12], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ("timestamp_['nt-ht', 'ct-ft', 'ts-dt']", 'int64'): [4, 4, 0, 1, 4, 4], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_levenshtein_multiple_value(self):
        df = self.df.copy()
        result = df.cols.levenshtein(cols=['last position seen', 'age', 'japanese name'], value=['10005', '000', "['Bumble']"])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_levenshtein_single_col(self):
        df = self.df.copy()
        result = df.cols.levenshtein(cols=['rank'], other_cols=['weight(t)'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [3, 3, 3, 2, 3, 3], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_levenshtein_single_value(self):
        df = self.df.copy()
        result = df.cols.levenshtein(cols=['names'], value='prime', output_cols='nms')
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('nms', 'int64'): [4, 11, 7, 8, 9, 11], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_match_rating_codex_all(self):
        df = self.df.copy()
        result = df.cols.match_rating_codex(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['N', 'N', 'N', 'N', 'N', 'N'], ('date arrival', 'object'): ['198/10', '198/10', '198/10', '198/10', '198/10', '198/10'], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'N', '30.0'], ('last date seen', 'object'): ['201/10', '201/10', '201/10', '201/10', '201/10', '201/10'], ('last position seen', 'object'): ['19.201', '10.534', '37.356', '3.6153', 'N', 'N'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['TR', 'TR', 'TR', 'TR', 'TR', 'FLS'], ('Date Type', 'object'): ['201-10', '201-10', '201-24', '201-24', '201-10', '201-10'], ('age', 'object'): ['50', '50', '50', '50', '50', '50'], ('function', 'object'): ['LDR', 'ESPNG', 'SCRTY', 'FRSTNT', 'N', 'BTLSTN'], ('names', 'object'): ['OPTMS', 'BMB#BÉ', 'MTRPLX', 'BMBLB', 'MÉTL-X', 'MTR)^$'], ('timestamp', 'object'): ['201-24', '201-24', '201-24', '201-24', '201-24', '201-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'N']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_match_rating_codex_multiple(self):
        df = self.df.copy()
        result = df.cols.match_rating_codex(cols=['NullType', 'Cybertronian', 'timestamp'], output_cols=['nt', 'ct', 'ts'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['N', 'N', 'N', 'N', 'N', 'N'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('ct', 'object'): ['TR', 'TR', 'TR', 'TR', 'TR', 'FLS'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('ts', 'object'): ['201-24', '201-24', '201-24', '201-24', '201-24', '201-24'], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_match_rating_codex_numeric(self):
        df = self.df.copy()
        result = df.cols.match_rating_codex(cols=['rank'], output_cols=['rk'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('rk', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_match_rating_codex_string(self):
        df = self.df.copy()
        result = df.cols.match_rating_codex(cols=['names'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['OPTMS', 'BMB#BÉ', 'MTRPLX', 'BMBLB', 'MÉTL-X', 'MTR)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_metaphone_all(self):
        df = self.df.copy()
        result = df.cols.metaphone(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['NN', 'NN', 'NN', 'NN', 'NN', 'NN'], ('date arrival', 'object'): ['', '', '', '', '', ''], ('height(ft)', 'object'): ['', '', '', '', 'NN', ''], ('last date seen', 'object'): ['', '', '', '', '', ''], ('last position seen', 'object'): ['', '', '', '', 'NN', 'NN'], ('rank', 'object'): ['', '', '', '', '', ''], ('Cybertronian', 'object'): ['TR', 'TR', 'TR', 'TR', 'TR', 'FLS'], ('Date Type', 'object'): ['', '', '', '', '', ''], ('age', 'object'): ['', '', '', '', '', ''], ('function', 'object'): ['LTR', 'ESPNJ', 'SKRT', 'FRST LTNNT', 'NN', 'BTL STXN'], ('names', 'object'): ['OPTMS', 'BMBLB ', 'MTRPLKS', 'BMBLB', 'MTRP LKS', 'MTRPLKS'], ('timestamp', 'object'): ['', '', '', '', '', ''], ('weight(t)', 'object'): ['', '', '', '', '', 'NN']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_metaphone_multiple(self):
        df = self.df.copy()
        result = df.cols.metaphone(cols=['NullType', 'Cybertronian', 'timestamp'], output_cols=['nt', 'ct', 'ts'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['NN', 'NN', 'NN', 'NN', 'NN', 'NN'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('ct', 'object'): ['TR', 'TR', 'TR', 'TR', 'TR', 'FLS'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('ts', 'object'): ['', '', '', '', '', ''], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_metaphone_numeric(self):
        df = self.df.copy()
        result = df.cols.metaphone(cols=['rank'], output_cols=['rk'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('rk', 'object'): ['', '', '', '', '', ''], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_metaphone_string(self):
        df = self.df.copy()
        result = df.cols.metaphone(cols=['names'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['OPTMS', 'BMBLB ', 'MTRPLKS', 'BMBLB', 'MTRP LKS', 'MTRPLKS'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ngram_fingerprint_all(self):
        df = self.df.copy()
        result = df.cols.ngram_fingerprint(cols='*')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ngram_fingerprint_multiple(self):
        df = self.df.copy()
        result = df.cols.ngram_fingerprint(cols=['NullType', 'Cybertronian', 'timestamp'], n_size=4, output_cols=['nt', 'ct', 'ts'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ngram_fingerprint_numeric(self):
        df = self.df.copy()
        result = df.cols.ngram_fingerprint(cols=['rank'], output_cols=['rk'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ngram_fingerprint_string(self):
        df = self.df.copy()
        result = df.cols.ngram_fingerprint(cols=['function(binary)'], n_size=25)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ngrams_multiple(self):
        df = self.df.copy()
        result = df.cols.ngrams(cols=['date arrival', 'japanese name', 'last date seen'], n_size=1, output_cols=['da', 'jn', 'lds'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ngrams_single(self):
        df = self.df.copy()
        result = df.cols.ngrams(cols=['names'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): [['Op', 'pt', 'ti', 'im', 'mu', 'us'], ['bu', 'um', 'mb', 'bl', 'l#', '#e', 'eb', 'bé', 'éé', 'é ', '  '], ['Me', 'et', 'tr', 'ro', 'op', 'pl', 'le', 'ex'], ['bu', 'um', 'mb', 'bl', 'le', 'eb', 'be', 'ee'], ['mé', 'ét', 'tr', 'ro', 'op', 'p´', '´l', 'le', 'e-', '-x'], ['Me', 'et', 'tr', 'ro', 'op', 'pl', 'le', 'ex', 'x_', '_)', ')^', '^$']], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_nysiis_all(self):
        df = self.df.copy()
        result = df.cols.nysiis(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['NAN', 'NAN', 'NAN', 'NAN', 'NAN', 'NAN'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'NAN', '30.0'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '201/04/10'], ('last position seen', 'object'): ['19.42735,-9.201', '10.642707,-71.612534', '37.789563,-12.40356', '3.6706,-17.84153', 'NAN', 'NAN'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['TR', 'TR', 'TR', 'TR', 'TR', 'FALS'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '201-04-10'], ('age', 'object'): ['50', '50', '50', '50', '50', '50'], ('function', 'object'): ['LADAR', 'ESPANAG', 'SACARATY', 'FARST', 'NAN', 'BATL'], ('names', 'object'): ['OPTAN', 'BANBL#ABÉ', 'MATRAPLAX', 'BANBLABY', 'MÉTRAP´LA-X', 'MATRAPLAX_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'NAN']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_nysiis_multiple(self):
        df = self.df.copy()
        result = df.cols.nysiis(cols=['NullType', 'Cybertronian', 'timestamp'], output_cols=['nt', 'ct', 'ts'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['NAN', 'NAN', 'NAN', 'NAN', 'NAN', 'NAN'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('ct', 'object'): ['TR', 'TR', 'TR', 'TR', 'TR', 'FALS'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('ts', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_nysiis_numeric(self):
        df = self.df.copy()
        result = df.cols.nysiis(cols=['rank'], output_cols=['rk'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('rk', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_nysiis_string(self):
        df = self.df.copy()
        result = df.cols.nysiis(cols=['names'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['OPTAN', 'BANBL#ABÉ', 'MATRAPLAX', 'BANBLABY', 'MÉTRAP´LA-X', 'MATRAPLAX_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pos_multiple(self):
        df = self.df.copy()
        result = df.cols.pos(cols=['date arrival', 'japanese name', 'last date seen'], output_cols=['da', 'jn', 'lds'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pos_single(self):
        df = self.df.copy()
        result = df.cols.pos(cols=['names'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): [[('Optimus', 'NN')], [('bumbl#ebéé', 'NN')], [('Metroplex', 'NNP')], [('bumblebee', 'NN')], [('métrop´le-x', 'NN')], [('Metroplex_)^$', 'NN')]], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_soundex_all(self):
        df = self.df.copy()
        result = df.cols.soundex(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['N500', 'N500', 'N500', 'N500', 'N500', 'N500'], ('date arrival', 'object'): ['1000', '1000', '1000', '1000', '1000', '1000'], ('height(ft)', 'object'): ['-000', '1000', '2000', '1000', 'N500', '3000'], ('last date seen', 'object'): ['2000', '2000', '2000', '2000', '2000', '2000'], ('last position seen', 'object'): ['1000', '1000', '3000', '3000', 'N500', 'N500'], ('rank', 'object'): ['1000', '7000', '7000', '8000', '1000', '8000'], ('Cybertronian', 'object'): ['T600', 'T600', 'T600', 'T600', 'T600', 'F420'], ('Date Type', 'object'): ['2000', '2000', '2000', '2000', '2000', '2000'], ('age', 'object'): ['5000', '5000', '5000', '5000', '5000', '5000'], ('function', 'object'): ['L360', 'E215', 'S263', 'F623', 'N500', 'B342'], ('names', 'object'): ['O135', 'B514', 'M361', 'B514', 'M361', 'M361'], ('timestamp', 'object'): ['2000', '2000', '2000', '2000', '2000', '2000'], ('weight(t)', 'object'): ['4000', '2000', '4000', '1000', '5000', 'N500']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_soundex_multiple(self):
        df = self.df.copy()
        result = df.cols.soundex(cols=['NullType', 'Cybertronian', 'timestamp'], output_cols=['nt', 'ct', 'ts'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['N500', 'N500', 'N500', 'N500', 'N500', 'N500'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('ct', 'object'): ['T600', 'T600', 'T600', 'T600', 'T600', 'F420'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('ts', 'object'): ['2000', '2000', '2000', '2000', '2000', '2000'], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_soundex_numeric(self):
        df = self.df.copy()
        result = df.cols.soundex(cols=['rank'], output_cols=['rk'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('rk', 'object'): ['1000', '7000', '7000', '8000', '1000', '8000'], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'Metroplex', 'bumblebee', 'métrop´le-x', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_soundex_string(self):
        df = self.df.copy()
        result = df.cols.soundex(cols=['names'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'], ('names', 'object'): ['O135', 'B514', 'M361', 'B514', 'M361', 'M361'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_all_double_metaphone(self):
        df = self.df.copy()
        result = df.string_clustering(cols='*', algorithm='double_metaphone')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_all_fingerprint(self):
        df = self.df.copy()
        result = df.string_clustering(cols='*', algorithm='fingerprint')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_all_levenshtein(self):
        df = self.df.copy()
        result = df.string_clustering(cols='*', algorithm='levenshtein')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_all_match_rating_codex(self):
        df = self.df.copy()
        result = df.string_clustering(cols='*', algorithm='match_rating_codex')
        expected = { 'Cybertronian': { 'FLS': { 'suggestion': 'False',
                             'suggestions': ['False'],
                             'suggestions_size': 1,
                             'total_count': 1},
                    'TR': { 'suggestion': 'True',
                            'suggestions': ['True'],
                            'suggestions_size': 1,
                            'total_count': 5}},
  'Date Type': { '201-10': { 'suggestion': '2016-09-10',
                             'suggestions': [ '2016-09-10',
                                              '2015-08-10',
                                              '2012-05-10',
                                              '2011-04-10'],
                             'suggestions_size': 4,
                             'total_count': 4},
                 '201-24': { 'suggestion': '2014-06-24',
                             'suggestions': ['2014-06-24', '2013-06-24'],
                             'suggestions_size': 2,
                             'total_count': 2}},
  'NullType': { 'N': { 'suggestion': 'None',
                       'suggestions': ['None'],
                       'suggestions_size': 1,
                       'total_count': 6}},
  'age': { '50': { 'suggestion': '5000000',
                   'suggestions': ['5000000'],
                   'suggestions_size': 1,
                   'total_count': 6}},
  'date arrival': { '198/10': { 'suggestion': '1980/04/10',
                                'suggestions': ['1980/04/10'],
                                'suggestions_size': 1,
                                'total_count': 6}},
  'function': { 'BTLSTN': { 'suggestion': 'Battle Station',
                            'suggestions': ['Battle Station'],
                            'suggestions_size': 1,
                            'total_count': 1},
                'ESPNG': { 'suggestion': 'Espionage',
                           'suggestions': ['Espionage'],
                           'suggestions_size': 1,
                           'total_count': 1},
                'FRSTNT': { 'suggestion': 'First Lieutenant',
                            'suggestions': ['First Lieutenant'],
                            'suggestions_size': 1,
                            'total_count': 1},
                'LDR': { 'suggestion': 'Leader',
                         'suggestions': ['Leader'],
                         'suggestions_size': 1,
                         'total_count': 1},
                'N': { 'suggestion': 'None',
                       'suggestions': ['None'],
                       'suggestions_size': 1,
                       'total_count': 1},
                'SCRTY': { 'suggestion': 'Security',
                           'suggestions': ['Security'],
                           'suggestions_size': 1,
                           'total_count': 1}},
  'height(ft)': { '-28.0': { 'suggestion': '-28.0',
                             'suggestions': ['-28.0'],
                             'suggestions_size': 1,
                             'total_count': 1},
                  '13.0': { 'suggestion': '13.0',
                            'suggestions': ['13.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  '17.0': { 'suggestion': '17.0',
                            'suggestions': ['17.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  '26.0': { 'suggestion': '26.0',
                            'suggestions': ['26.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  '30.0': { 'suggestion': '300.0',
                            'suggestions': ['300.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  'N': { 'suggestion': 'nan',
                         'suggestions': ['nan'],
                         'suggestions_size': 1,
                         'total_count': 1}},
  'last date seen': { '201/10': { 'suggestion': '2016/09/10',
                                  'suggestions': [ '2016/09/10',
                                                   '2015/08/10',
                                                   '2014/07/10',
                                                   '2013/06/10',
                                                   '2012/05/10',
                                                   '2011/04/10'],
                                  'suggestions_size': 6,
                                  'total_count': 6}},
  'last position seen': { '10.534': { 'suggestion': '10.642707,-71.612534',
                                      'suggestions': ['10.642707,-71.612534'],
                                      'suggestions_size': 1,
                                      'total_count': 1},
                          '19.201': { 'suggestion': '19.442735,-99.201111',
                                      'suggestions': ['19.442735,-99.201111'],
                                      'suggestions_size': 1,
                                      'total_count': 1},
                          '3.6153': { 'suggestion': '33.670666,-117.841553',
                                      'suggestions': ['33.670666,-117.841553'],
                                      'suggestions_size': 1,
                                      'total_count': 1},
                          '37.356': { 'suggestion': '37.789563,-122.400356',
                                      'suggestions': ['37.789563,-122.400356'],
                                      'suggestions_size': 1,
                                      'total_count': 1},
                          'N': { 'suggestion': 'None',
                                 'suggestions': ['None'],
                                 'suggestions_size': 1,
                                 'total_count': 2}},
  'names': { 'BMB#BÉ': { 'suggestion': 'bumbl#ebéé  ',
                         'suggestions': ['bumbl#ebéé  '],
                         'suggestions_size': 1,
                         'total_count': 1},
             'BMBLB': { 'suggestion': 'bumblebee',
                        'suggestions': ['bumblebee'],
                        'suggestions_size': 1,
                        'total_count': 1},
             'MTR)^$': { 'suggestion': 'Metroplex_)^$',
                         'suggestions': ['Metroplex_)^$'],
                         'suggestions_size': 1,
                         'total_count': 1},
             'MTRPLX': { 'suggestion': 'Metroplex',
                         'suggestions': ['Metroplex'],
                         'suggestions_size': 1,
                         'total_count': 1},
             'MÉTL-X': { 'suggestion': 'métrop´le-x',
                         'suggestions': ['métrop´le-x'],
                         'suggestions_size': 1,
                         'total_count': 1},
             'OPTMS': { 'suggestion': 'Optimus',
                        'suggestions': ['Optimus'],
                        'suggestions_size': 1,
                        'total_count': 1}},
  'rank': { '10': { 'suggestion': '10',
                    'suggestions': ['10'],
                    'suggestions_size': 1,
                    'total_count': 2},
            '7': { 'suggestion': '7',
                   'suggestions': ['7'],
                   'suggestions_size': 1,
                   'total_count': 2},
            '8': { 'suggestion': '8',
                   'suggestions': ['8'],
                   'suggestions_size': 1,
                   'total_count': 2}},
  'timestamp': { '201-24': { 'suggestion': '2014-06-24',
                             'suggestions': ['2014-06-24'],
                             'suggestions_size': 1,
                             'total_count': 6}},
  'weight(t)': { '1.8': { 'suggestion': '1.8',
                          'suggestions': ['1.8'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 '2.0': { 'suggestion': '2.0',
                          'suggestions': ['2.0'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 '4.0': { 'suggestion': '4.0',
                          'suggestions': ['4.0'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 '4.3': { 'suggestion': '4.3',
                          'suggestions': ['4.3'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 '5.7': { 'suggestion': '5.7',
                          'suggestions': ['5.7'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 'N': { 'suggestion': 'nan',
                        'suggestions': ['nan'],
                        'suggestions_size': 1,
                        'total_count': 1}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_all_metaphone(self):
        df = self.df.copy()
        result = df.string_clustering(cols='*', algorithm='metaphone')
        expected = { 'Cybertronian': { 'FLS': { 'suggestion': 'False',
                             'suggestions': ['False'],
                             'suggestions_size': 1,
                             'total_count': 1},
                    'TR': { 'suggestion': 'True',
                            'suggestions': ['True'],
                            'suggestions_size': 1,
                            'total_count': 5}},
  'Date Type': { '': { 'suggestion': '2016-09-10',
                       'suggestions': [ '2016-09-10',
                                        '2015-08-10',
                                        '2014-06-24',
                                        '2013-06-24',
                                        '2012-05-10',
                                        '2011-04-10'],
                       'suggestions_size': 6,
                       'total_count': 6}},
  'NullType': { 'NN': { 'suggestion': 'None',
                        'suggestions': ['None'],
                        'suggestions_size': 1,
                        'total_count': 6}},
  'age': { '': { 'suggestion': '5000000',
                 'suggestions': ['5000000'],
                 'suggestions_size': 1,
                 'total_count': 6}},
  'date arrival': { '': { 'suggestion': '1980/04/10',
                          'suggestions': ['1980/04/10'],
                          'suggestions_size': 1,
                          'total_count': 6}},
  'function': { 'BTL STXN': { 'suggestion': 'Battle Station',
                              'suggestions': ['Battle Station'],
                              'suggestions_size': 1,
                              'total_count': 1},
                'ESPNJ': { 'suggestion': 'Espionage',
                           'suggestions': ['Espionage'],
                           'suggestions_size': 1,
                           'total_count': 1},
                'FRST LTNNT': { 'suggestion': 'First Lieutenant',
                                'suggestions': ['First Lieutenant'],
                                'suggestions_size': 1,
                                'total_count': 1},
                'LTR': { 'suggestion': 'Leader',
                         'suggestions': ['Leader'],
                         'suggestions_size': 1,
                         'total_count': 1},
                'NN': { 'suggestion': 'None',
                        'suggestions': ['None'],
                        'suggestions_size': 1,
                        'total_count': 1},
                'SKRT': { 'suggestion': 'Security',
                          'suggestions': ['Security'],
                          'suggestions_size': 1,
                          'total_count': 1}},
  'height(ft)': { '': { 'suggestion': '-28.0',
                        'suggestions': [ '-28.0',
                                         '17.0',
                                         '26.0',
                                         '13.0',
                                         '300.0'],
                        'suggestions_size': 5,
                        'total_count': 5},
                  'NN': { 'suggestion': 'nan',
                          'suggestions': ['nan'],
                          'suggestions_size': 1,
                          'total_count': 1}},
  'last date seen': { '': { 'suggestion': '2016/09/10',
                            'suggestions': [ '2016/09/10',
                                             '2015/08/10',
                                             '2014/07/10',
                                             '2013/06/10',
                                             '2012/05/10',
                                             '2011/04/10'],
                            'suggestions_size': 6,
                            'total_count': 6}},
  'last position seen': { '': { 'suggestion': '19.442735,-99.201111',
                                'suggestions': [ '19.442735,-99.201111',
                                                 '10.642707,-71.612534',
                                                 '37.789563,-122.400356',
                                                 '33.670666,-117.841553'],
                                'suggestions_size': 4,
                                'total_count': 4},
                          'NN': { 'suggestion': 'None',
                                  'suggestions': ['None'],
                                  'suggestions_size': 1,
                                  'total_count': 2}},
  'names': { 'BMBLB': { 'suggestion': 'bumblebee',
                        'suggestions': ['bumblebee'],
                        'suggestions_size': 1,
                        'total_count': 1},
             'BMBLB ': { 'suggestion': 'bumbl#ebéé  ',
                         'suggestions': ['bumbl#ebéé  '],
                         'suggestions_size': 1,
                         'total_count': 1},
             'MTRP LKS': { 'suggestion': 'métrop´le-x',
                           'suggestions': ['métrop´le-x'],
                           'suggestions_size': 1,
                           'total_count': 1},
             'MTRPLKS': { 'suggestion': 'Metroplex',
                          'suggestions': ['Metroplex', 'Metroplex_)^$'],
                          'suggestions_size': 2,
                          'total_count': 2},
             'OPTMS': { 'suggestion': 'Optimus',
                        'suggestions': ['Optimus'],
                        'suggestions_size': 1,
                        'total_count': 1}},
  'rank': { '': { 'suggestion': '10',
                  'suggestions': ['10', '7', '8'],
                  'suggestions_size': 3,
                  'total_count': 6}},
  'timestamp': { '': { 'suggestion': '2014-06-24',
                       'suggestions': ['2014-06-24'],
                       'suggestions_size': 1,
                       'total_count': 6}},
  'weight(t)': { '': { 'suggestion': '4.3',
                       'suggestions': ['4.3', '2.0', '4.0', '1.8', '5.7'],
                       'suggestions_size': 5,
                       'total_count': 5},
                 'NN': { 'suggestion': 'nan',
                         'suggestions': ['nan'],
                         'suggestions_size': 1,
                         'total_count': 1}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_all_ngram_fingerprint(self):
        df = self.df.copy()
        result = df.string_clustering(cols='*', algorithm='ngram_fingerprint')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_all_nysiis(self):
        df = self.df.copy()
        result = df.string_clustering(cols='*', algorithm='nysiis')
        expected = { 'Cybertronian': { 'FALS': { 'suggestion': 'False',
                              'suggestions': ['False'],
                              'suggestions_size': 1,
                              'total_count': 1},
                    'TR': { 'suggestion': 'True',
                            'suggestions': ['True'],
                            'suggestions_size': 1,
                            'total_count': 5}},
  'Date Type': { '201-04-10': { 'suggestion': '2011-04-10',
                                'suggestions': ['2011-04-10'],
                                'suggestions_size': 1,
                                'total_count': 1},
                 '2012-05-10': { 'suggestion': '2012-05-10',
                                 'suggestions': ['2012-05-10'],
                                 'suggestions_size': 1,
                                 'total_count': 1},
                 '2013-06-24': { 'suggestion': '2013-06-24',
                                 'suggestions': ['2013-06-24'],
                                 'suggestions_size': 1,
                                 'total_count': 1},
                 '2014-06-24': { 'suggestion': '2014-06-24',
                                 'suggestions': ['2014-06-24'],
                                 'suggestions_size': 1,
                                 'total_count': 1},
                 '2015-08-10': { 'suggestion': '2015-08-10',
                                 'suggestions': ['2015-08-10'],
                                 'suggestions_size': 1,
                                 'total_count': 1},
                 '2016-09-10': { 'suggestion': '2016-09-10',
                                 'suggestions': ['2016-09-10'],
                                 'suggestions_size': 1,
                                 'total_count': 1}},
  'NullType': { 'NAN': { 'suggestion': 'None',
                         'suggestions': ['None'],
                         'suggestions_size': 1,
                         'total_count': 6}},
  'age': { '50': { 'suggestion': '5000000',
                   'suggestions': ['5000000'],
                   'suggestions_size': 1,
                   'total_count': 6}},
  'date arrival': { '1980/04/10': { 'suggestion': '1980/04/10',
                                    'suggestions': ['1980/04/10'],
                                    'suggestions_size': 1,
                                    'total_count': 6}},
  'function': { 'BATL': { 'suggestion': 'Battle Station',
                          'suggestions': ['Battle Station'],
                          'suggestions_size': 1,
                          'total_count': 1},
                'ESPANAG': { 'suggestion': 'Espionage',
                             'suggestions': ['Espionage'],
                             'suggestions_size': 1,
                             'total_count': 1},
                'FARST': { 'suggestion': 'First Lieutenant',
                           'suggestions': ['First Lieutenant'],
                           'suggestions_size': 1,
                           'total_count': 1},
                'LADAR': { 'suggestion': 'Leader',
                           'suggestions': ['Leader'],
                           'suggestions_size': 1,
                           'total_count': 1},
                'NAN': { 'suggestion': 'None',
                         'suggestions': ['None'],
                         'suggestions_size': 1,
                         'total_count': 1},
                'SACARATY': { 'suggestion': 'Security',
                              'suggestions': ['Security'],
                              'suggestions_size': 1,
                              'total_count': 1}},
  'height(ft)': { '-28.0': { 'suggestion': '-28.0',
                             'suggestions': ['-28.0'],
                             'suggestions_size': 1,
                             'total_count': 1},
                  '13.0': { 'suggestion': '13.0',
                            'suggestions': ['13.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  '17.0': { 'suggestion': '17.0',
                            'suggestions': ['17.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  '26.0': { 'suggestion': '26.0',
                            'suggestions': ['26.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  '30.0': { 'suggestion': '300.0',
                            'suggestions': ['300.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  'NAN': { 'suggestion': 'nan',
                           'suggestions': ['nan'],
                           'suggestions_size': 1,
                           'total_count': 1}},
  'last date seen': { '201/04/10': { 'suggestion': '2011/04/10',
                                     'suggestions': ['2011/04/10'],
                                     'suggestions_size': 1,
                                     'total_count': 1},
                      '2012/05/10': { 'suggestion': '2012/05/10',
                                      'suggestions': ['2012/05/10'],
                                      'suggestions_size': 1,
                                      'total_count': 1},
                      '2013/06/10': { 'suggestion': '2013/06/10',
                                      'suggestions': ['2013/06/10'],
                                      'suggestions_size': 1,
                                      'total_count': 1},
                      '2014/07/10': { 'suggestion': '2014/07/10',
                                      'suggestions': ['2014/07/10'],
                                      'suggestions_size': 1,
                                      'total_count': 1},
                      '2015/08/10': { 'suggestion': '2015/08/10',
                                      'suggestions': ['2015/08/10'],
                                      'suggestions_size': 1,
                                      'total_count': 1},
                      '2016/09/10': { 'suggestion': '2016/09/10',
                                      'suggestions': ['2016/09/10'],
                                      'suggestions_size': 1,
                                      'total_count': 1}},
  'last position seen': { '10.642707,-71.612534': { 'suggestion': '10.642707,-71.612534',
                                                    'suggestions': [ '10.642707,-71.612534'],
                                                    'suggestions_size': 1,
                                                    'total_count': 1},
                          '19.42735,-9.201': { 'suggestion': '19.442735,-99.201111',
                                               'suggestions': [ '19.442735,-99.201111'],
                                               'suggestions_size': 1,
                                               'total_count': 1},
                          '3.6706,-17.84153': { 'suggestion': '33.670666,-117.841553',
                                                'suggestions': [ '33.670666,-117.841553'],
                                                'suggestions_size': 1,
                                                'total_count': 1},
                          '37.789563,-12.40356': { 'suggestion': '37.789563,-122.400356',
                                                   'suggestions': [ '37.789563,-122.400356'],
                                                   'suggestions_size': 1,
                                                   'total_count': 1},
                          'NAN': { 'suggestion': 'None',
                                   'suggestions': ['None'],
                                   'suggestions_size': 1,
                                   'total_count': 2}},
  'names': { 'BANBL#ABÉ': { 'suggestion': 'bumbl#ebéé  ',
                            'suggestions': ['bumbl#ebéé  '],
                            'suggestions_size': 1,
                            'total_count': 1},
             'BANBLABY': { 'suggestion': 'bumblebee',
                           'suggestions': ['bumblebee'],
                           'suggestions_size': 1,
                           'total_count': 1},
             'MATRAPLAX': { 'suggestion': 'Metroplex',
                            'suggestions': ['Metroplex'],
                            'suggestions_size': 1,
                            'total_count': 1},
             'MATRAPLAX_)^$': { 'suggestion': 'Metroplex_)^$',
                                'suggestions': ['Metroplex_)^$'],
                                'suggestions_size': 1,
                                'total_count': 1},
             'MÉTRAP´LA-X': { 'suggestion': 'métrop´le-x',
                              'suggestions': ['métrop´le-x'],
                              'suggestions_size': 1,
                              'total_count': 1},
             'OPTAN': { 'suggestion': 'Optimus',
                        'suggestions': ['Optimus'],
                        'suggestions_size': 1,
                        'total_count': 1}},
  'rank': { '10': { 'suggestion': '10',
                    'suggestions': ['10'],
                    'suggestions_size': 1,
                    'total_count': 2},
            '7': { 'suggestion': '7',
                   'suggestions': ['7'],
                   'suggestions_size': 1,
                   'total_count': 2},
            '8': { 'suggestion': '8',
                   'suggestions': ['8'],
                   'suggestions_size': 1,
                   'total_count': 2}},
  'timestamp': { '2014-06-24': { 'suggestion': '2014-06-24',
                                 'suggestions': ['2014-06-24'],
                                 'suggestions_size': 1,
                                 'total_count': 6}},
  'weight(t)': { '1.8': { 'suggestion': '1.8',
                          'suggestions': ['1.8'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 '2.0': { 'suggestion': '2.0',
                          'suggestions': ['2.0'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 '4.0': { 'suggestion': '4.0',
                          'suggestions': ['4.0'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 '4.3': { 'suggestion': '4.3',
                          'suggestions': ['4.3'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 '5.7': { 'suggestion': '5.7',
                          'suggestions': ['5.7'],
                          'suggestions_size': 1,
                          'total_count': 1},
                 'NAN': { 'suggestion': 'nan',
                          'suggestions': ['nan'],
                          'suggestions_size': 1,
                          'total_count': 1}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_all_soundex(self):
        df = self.df.copy()
        result = df.string_clustering(cols='*', algorithm='soundex')
        expected = { 'Cybertronian': { 'F420': { 'suggestion': 'False',
                              'suggestions': ['False'],
                              'suggestions_size': 1,
                              'total_count': 1},
                    'T600': { 'suggestion': 'True',
                              'suggestions': ['True'],
                              'suggestions_size': 1,
                              'total_count': 5}},
  'Date Type': { '2000': { 'suggestion': '2016-09-10',
                           'suggestions': [ '2016-09-10',
                                            '2015-08-10',
                                            '2014-06-24',
                                            '2013-06-24',
                                            '2012-05-10',
                                            '2011-04-10'],
                           'suggestions_size': 6,
                           'total_count': 6}},
  'NullType': { 'N500': { 'suggestion': 'None',
                          'suggestions': ['None'],
                          'suggestions_size': 1,
                          'total_count': 6}},
  'age': { '5000': { 'suggestion': '5000000',
                     'suggestions': ['5000000'],
                     'suggestions_size': 1,
                     'total_count': 6}},
  'date arrival': { '1000': { 'suggestion': '1980/04/10',
                              'suggestions': ['1980/04/10'],
                              'suggestions_size': 1,
                              'total_count': 6}},
  'function': { 'B342': { 'suggestion': 'Battle Station',
                          'suggestions': ['Battle Station'],
                          'suggestions_size': 1,
                          'total_count': 1},
                'E215': { 'suggestion': 'Espionage',
                          'suggestions': ['Espionage'],
                          'suggestions_size': 1,
                          'total_count': 1},
                'F623': { 'suggestion': 'First Lieutenant',
                          'suggestions': ['First Lieutenant'],
                          'suggestions_size': 1,
                          'total_count': 1},
                'L360': { 'suggestion': 'Leader',
                          'suggestions': ['Leader'],
                          'suggestions_size': 1,
                          'total_count': 1},
                'N500': { 'suggestion': 'None',
                          'suggestions': ['None'],
                          'suggestions_size': 1,
                          'total_count': 1},
                'S263': { 'suggestion': 'Security',
                          'suggestions': ['Security'],
                          'suggestions_size': 1,
                          'total_count': 1}},
  'height(ft)': { '-000': { 'suggestion': '-28.0',
                            'suggestions': ['-28.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  '1000': { 'suggestion': '17.0',
                            'suggestions': ['17.0', '13.0'],
                            'suggestions_size': 2,
                            'total_count': 2},
                  '2000': { 'suggestion': '26.0',
                            'suggestions': ['26.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  '3000': { 'suggestion': '300.0',
                            'suggestions': ['300.0'],
                            'suggestions_size': 1,
                            'total_count': 1},
                  'N500': { 'suggestion': 'nan',
                            'suggestions': ['nan'],
                            'suggestions_size': 1,
                            'total_count': 1}},
  'last date seen': { '2000': { 'suggestion': '2016/09/10',
                                'suggestions': [ '2016/09/10',
                                                 '2015/08/10',
                                                 '2014/07/10',
                                                 '2013/06/10',
                                                 '2012/05/10',
                                                 '2011/04/10'],
                                'suggestions_size': 6,
                                'total_count': 6}},
  'last position seen': { '1000': { 'suggestion': '19.442735,-99.201111',
                                    'suggestions': [ '19.442735,-99.201111',
                                                     '10.642707,-71.612534'],
                                    'suggestions_size': 2,
                                    'total_count': 2},
                          '3000': { 'suggestion': '37.789563,-122.400356',
                                    'suggestions': [ '37.789563,-122.400356',
                                                     '33.670666,-117.841553'],
                                    'suggestions_size': 2,
                                    'total_count': 2},
                          'N500': { 'suggestion': 'None',
                                    'suggestions': ['None'],
                                    'suggestions_size': 1,
                                    'total_count': 2}},
  'names': { 'B514': { 'suggestion': 'bumbl#ebéé  ',
                       'suggestions': ['bumbl#ebéé  ', 'bumblebee'],
                       'suggestions_size': 2,
                       'total_count': 2},
             'M361': { 'suggestion': 'Metroplex',
                       'suggestions': [ 'Metroplex',
                                        'métrop´le-x',
                                        'Metroplex_)^$'],
                       'suggestions_size': 3,
                       'total_count': 3},
             'O135': { 'suggestion': 'Optimus',
                       'suggestions': ['Optimus'],
                       'suggestions_size': 1,
                       'total_count': 1}},
  'rank': { '1000': { 'suggestion': '10',
                      'suggestions': ['10'],
                      'suggestions_size': 1,
                      'total_count': 2},
            '7000': { 'suggestion': '7',
                      'suggestions': ['7'],
                      'suggestions_size': 1,
                      'total_count': 2},
            '8000': { 'suggestion': '8',
                      'suggestions': ['8'],
                      'suggestions_size': 1,
                      'total_count': 2}},
  'timestamp': { '2000': { 'suggestion': '2014-06-24',
                           'suggestions': ['2014-06-24'],
                           'suggestions_size': 1,
                           'total_count': 6}},
  'weight(t)': { '1000': { 'suggestion': '1.8',
                           'suggestions': ['1.8'],
                           'suggestions_size': 1,
                           'total_count': 1},
                 '2000': { 'suggestion': '2.0',
                           'suggestions': ['2.0'],
                           'suggestions_size': 1,
                           'total_count': 1},
                 '4000': { 'suggestion': '4.3',
                           'suggestions': ['4.3', '4.0'],
                           'suggestions_size': 2,
                           'total_count': 2},
                 '5000': { 'suggestion': '5.7',
                           'suggestions': ['5.7'],
                           'suggestions_size': 1,
                           'total_count': 1},
                 'N500': { 'suggestion': 'nan',
                           'suggestions': ['nan'],
                           'suggestions_size': 1,
                           'total_count': 1}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_multiple_double_metaphone(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['NullType', 'Cybertronian', 'timestamp'], algorithm='double_metaphone')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_multiple_fingerprint(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['NullType', 'Cybertronian', 'timestamp'], algorithm='fingerprint')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_multiple_levenshtein(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['NullType', 'Cybertronian', 'timestamp'], algorithm='levenshtein')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_multiple_match_rating_codex(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['NullType', 'Cybertronian', 'timestamp'], algorithm='match_rating_codex')
        expected = { 'Cybertronian': { 'FLS': { 'suggestion': 'False',
                             'suggestions': ['False'],
                             'suggestions_size': 1,
                             'total_count': 1},
                    'TR': { 'suggestion': 'True',
                            'suggestions': ['True'],
                            'suggestions_size': 1,
                            'total_count': 5}},
  'NullType': { 'N': { 'suggestion': 'None',
                       'suggestions': ['None'],
                       'suggestions_size': 1,
                       'total_count': 6}},
  'timestamp': { '201-24': { 'suggestion': '2014-06-24',
                             'suggestions': ['2014-06-24'],
                             'suggestions_size': 1,
                             'total_count': 6}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_multiple_metaphone(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['NullType', 'Cybertronian', 'timestamp'], algorithm='metaphone')
        expected = { 'Cybertronian': { 'FLS': { 'suggestion': 'False',
                             'suggestions': ['False'],
                             'suggestions_size': 1,
                             'total_count': 1},
                    'TR': { 'suggestion': 'True',
                            'suggestions': ['True'],
                            'suggestions_size': 1,
                            'total_count': 5}},
  'NullType': { 'NN': { 'suggestion': 'None',
                        'suggestions': ['None'],
                        'suggestions_size': 1,
                        'total_count': 6}},
  'timestamp': { '': { 'suggestion': '2014-06-24',
                       'suggestions': ['2014-06-24'],
                       'suggestions_size': 1,
                       'total_count': 6}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_multiple_ngram_fingerprint(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['NullType', 'Cybertronian', 'timestamp'], algorithm='ngram_fingerprint')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_multiple_nysiis(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['NullType', 'Cybertronian', 'timestamp'], algorithm='nysiis')
        expected = { 'Cybertronian': { 'FALS': { 'suggestion': 'False',
                              'suggestions': ['False'],
                              'suggestions_size': 1,
                              'total_count': 1},
                    'TR': { 'suggestion': 'True',
                            'suggestions': ['True'],
                            'suggestions_size': 1,
                            'total_count': 5}},
  'NullType': { 'NAN': { 'suggestion': 'None',
                         'suggestions': ['None'],
                         'suggestions_size': 1,
                         'total_count': 6}},
  'timestamp': { '2014-06-24': { 'suggestion': '2014-06-24',
                                 'suggestions': ['2014-06-24'],
                                 'suggestions_size': 1,
                                 'total_count': 6}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_multiple_soundex(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['NullType', 'Cybertronian', 'timestamp'], algorithm='soundex')
        expected = { 'Cybertronian': { 'F420': { 'suggestion': 'False',
                              'suggestions': ['False'],
                              'suggestions_size': 1,
                              'total_count': 1},
                    'T600': { 'suggestion': 'True',
                              'suggestions': ['True'],
                              'suggestions_size': 1,
                              'total_count': 5}},
  'NullType': { 'N500': { 'suggestion': 'None',
                          'suggestions': ['None'],
                          'suggestions_size': 1,
                          'total_count': 6}},
  'timestamp': { '2000': { 'suggestion': '2014-06-24',
                           'suggestions': ['2014-06-24'],
                           'suggestions_size': 1,
                           'total_count': 6}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_numeric_double_metaphone(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['rank'], algorithm='double_metaphone')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_numeric_fingerprint(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['rank'], algorithm='fingerprint')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_numeric_levenshtein(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['rank'], algorithm='levenshtein')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_numeric_match_rating_codex(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['rank'], algorithm='match_rating_codex')
        expected = { 'rank': { '10': { 'suggestion': '10',
                    'suggestions': ['10'],
                    'suggestions_size': 1,
                    'total_count': 2},
            '7': { 'suggestion': '7',
                   'suggestions': ['7'],
                   'suggestions_size': 1,
                   'total_count': 2},
            '8': { 'suggestion': '8',
                   'suggestions': ['8'],
                   'suggestions_size': 1,
                   'total_count': 2}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_numeric_metaphone(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['rank'], algorithm='metaphone')
        expected = { 'rank': { '': { 'suggestion': '10',
                  'suggestions': ['10', '7', '8'],
                  'suggestions_size': 3,
                  'total_count': 6}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_numeric_ngram_fingerprint(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['rank'], algorithm='ngram_fingerprint')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_numeric_nysiis(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['rank'], algorithm='nysiis')
        expected = { 'rank': { '10': { 'suggestion': '10',
                    'suggestions': ['10'],
                    'suggestions_size': 1,
                    'total_count': 2},
            '7': { 'suggestion': '7',
                   'suggestions': ['7'],
                   'suggestions_size': 1,
                   'total_count': 2},
            '8': { 'suggestion': '8',
                   'suggestions': ['8'],
                   'suggestions_size': 1,
                   'total_count': 2}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_numeric_soundex(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['rank'], algorithm='soundex')
        expected = { 'rank': { '1000': { 'suggestion': '10',
                      'suggestions': ['10'],
                      'suggestions_size': 1,
                      'total_count': 2},
            '7000': { 'suggestion': '7',
                      'suggestions': ['7'],
                      'suggestions_size': 1,
                      'total_count': 2},
            '8000': { 'suggestion': '8',
                      'suggestions': ['8'],
                      'suggestions_size': 1,
                      'total_count': 2}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_string_double_metaphone(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['names'], algorithm='double_metaphone')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_string_fingerprint(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['names'], algorithm='fingerprint')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_string_levenshtein(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['names'], algorithm='levenshtein')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_string_match_rating_codex(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['names'], algorithm='match_rating_codex')
        expected = { 'names': { 'BMB#BÉ': { 'suggestion': 'bumbl#ebéé  ',
                         'suggestions': ['bumbl#ebéé  '],
                         'suggestions_size': 1,
                         'total_count': 1},
             'BMBLB': { 'suggestion': 'bumblebee',
                        'suggestions': ['bumblebee'],
                        'suggestions_size': 1,
                        'total_count': 1},
             'MTR)^$': { 'suggestion': 'Metroplex_)^$',
                         'suggestions': ['Metroplex_)^$'],
                         'suggestions_size': 1,
                         'total_count': 1},
             'MTRPLX': { 'suggestion': 'Metroplex',
                         'suggestions': ['Metroplex'],
                         'suggestions_size': 1,
                         'total_count': 1},
             'MÉTL-X': { 'suggestion': 'métrop´le-x',
                         'suggestions': ['métrop´le-x'],
                         'suggestions_size': 1,
                         'total_count': 1},
             'OPTMS': { 'suggestion': 'Optimus',
                        'suggestions': ['Optimus'],
                        'suggestions_size': 1,
                        'total_count': 1}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_string_metaphone(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['names'], algorithm='metaphone')
        expected = { 'names': { 'BMBLB': { 'suggestion': 'bumblebee',
                        'suggestions': ['bumblebee'],
                        'suggestions_size': 1,
                        'total_count': 1},
             'BMBLB ': { 'suggestion': 'bumbl#ebéé  ',
                         'suggestions': ['bumbl#ebéé  '],
                         'suggestions_size': 1,
                         'total_count': 1},
             'MTRP LKS': { 'suggestion': 'métrop´le-x',
                           'suggestions': ['métrop´le-x'],
                           'suggestions_size': 1,
                           'total_count': 1},
             'MTRPLKS': { 'suggestion': 'Metroplex',
                          'suggestions': ['Metroplex', 'Metroplex_)^$'],
                          'suggestions_size': 2,
                          'total_count': 2},
             'OPTMS': { 'suggestion': 'Optimus',
                        'suggestions': ['Optimus'],
                        'suggestions_size': 1,
                        'total_count': 1}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_string_ngram_fingerprint(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['names'], algorithm='ngram_fingerprint')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_string_clustering_string_nysiis(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['names'], algorithm='nysiis')
        expected = { 'names': { 'BANBL#ABÉ': { 'suggestion': 'bumbl#ebéé  ',
                            'suggestions': ['bumbl#ebéé  '],
                            'suggestions_size': 1,
                            'total_count': 1},
             'BANBLABY': { 'suggestion': 'bumblebee',
                           'suggestions': ['bumblebee'],
                           'suggestions_size': 1,
                           'total_count': 1},
             'MATRAPLAX': { 'suggestion': 'Metroplex',
                            'suggestions': ['Metroplex'],
                            'suggestions_size': 1,
                            'total_count': 1},
             'MATRAPLAX_)^$': { 'suggestion': 'Metroplex_)^$',
                                'suggestions': ['Metroplex_)^$'],
                                'suggestions_size': 1,
                                'total_count': 1},
             'MÉTRAP´LA-X': { 'suggestion': 'métrop´le-x',
                              'suggestions': ['métrop´le-x'],
                              'suggestions_size': 1,
                              'total_count': 1},
             'OPTAN': { 'suggestion': 'Optimus',
                        'suggestions': ['Optimus'],
                        'suggestions_size': 1,
                        'total_count': 1}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_string_clustering_string_soundex(self):
        df = self.df.copy()
        result = df.string_clustering(cols=['names'], algorithm='soundex')
        expected = { 'names': { 'B514': { 'suggestion': 'bumbl#ebéé  ',
                       'suggestions': ['bumbl#ebéé  ', 'bumblebee'],
                       'suggestions_size': 2,
                       'total_count': 2},
             'M361': { 'suggestion': 'Metroplex',
                       'suggestions': [ 'Metroplex',
                                        'métrop´le-x',
                                        'Metroplex_)^$'],
                       'suggestions_size': 3,
                       'total_count': 3},
             'O135': { 'suggestion': 'Optimus',
                       'suggestions': ['Optimus'],
                       'suggestions_size': 1,
                       'total_count': 1}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))


class TestStringclusteringDask(TestStringclusteringPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestStringclusteringPartitionDask(TestStringclusteringPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringclusteringCUDF(TestStringclusteringPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringclusteringDC(TestStringclusteringPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringclusteringPartitionDC(TestStringclusteringPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringclusteringSpark(TestStringclusteringPandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringclusteringVaex(TestStringclusteringPandas):
        config = {'engine': 'vaex'}
