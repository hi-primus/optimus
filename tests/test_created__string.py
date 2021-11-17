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


class TestStringPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None

    def test_cols_capitalize(self):
        df = self.create_dataframe(data={('capitalize_test', 'object'): ['ThIs iS A TEST', 'ThIs', 'iS', 'a ', ' TEST', 'this is a test']}, force_data_types=True)
        result = df.cols.capitalize(cols=['capitalize_test'])
        expected = self.create_dataframe(data={('capitalize_test', 'object'): ['This is a test', 'This', 'Is', 'A ', ' test', 'This is a test']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_capitalize_all(self):
        df = self.df.copy()
        result = df.cols.capitalize(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[none, 5700.0]', '[91.44, none]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["Bytearray(b'leader')", "Bytearray(b'espionage')", "Bytearray(b'security')", "Bytearray(b'first lieutenant')", "Bytearray(b'none')", "Bytearray(b'battle station')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0'], ('japanese name', 'object'): ["['inochi', 'convoy']", "['bumble', 'goldback']", "['roadbuster']", "['meister']", "['megatron']", "['metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First lieutenant', 'None', 'Battle station'], ('names', 'object'): ['Optimus', 'Bumbl#ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_capitalize_multiple(self):
        df = self.df.copy()
        result = df.cols.capitalize(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['inochi', 'convoy']", "['bumble', 'goldback']", "['roadbuster']", "['meister']", "['megatron']", "['metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_capitalize_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.capitalize(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_capitalize_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.capitalize(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'Bumbl#ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_expand_contracted_words(self):
        df = self.create_dataframe(data={('expand_contracted_words_test', 'object'): ["y'all ain't ready for this", "i've been where you haven't", "SHE'LL DO IT BEFORE YOU", "maybe it isn't so hard after all", "he mustn't cheat in school", "IF YOU HADN'T DONE THAT, WE WOULD'VE BEEN FREE"]}, force_data_types=True)
        result = df.cols.expand_contracted_words(cols=['expand_contracted_words_test'])
        expected = self.create_dataframe(data={('expand_contracted_words_test', 'object'): ['you all am not ready for this', 'I have been where you have not', 'she will DO IT BEFORE YOU', 'maybe it is not so hard after all', 'he must not cheat in school', 'IF YOU had not DONE THAT, WE would have BEEN FREE']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_expand_contracted_words_all(self):
        df = self.df.copy()
        result = df.cols.expand_contracted_words(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'], ('japanese name', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_expand_contracted_words_multiple(self):
        df = self.df.copy()
        result = df.cols.expand_contracted_words(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_expand_contracted_words_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.expand_contracted_words(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_expand_contracted_words_string(self):
        df = self.df.copy().cols.select(['function(binary)'])
        result = df.cols.expand_contracted_words(cols=['function(binary)'], output_cols=['function(binary)_2'])
        expected = self.create_dataframe(data={('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('function(binary)_2', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lemmatize_verbs(self):
        df = self.create_dataframe(data={('lemmatize_verbs_test', 'object'): ['the players are tired of studying', 'feet and teeth sound simliar', 'it is us connected against the world', 'leave it alone', 'living in the world', 'I am aware that discoveries come very often nowadays']}, force_data_types=True)
        result = df.cols.lemmatize_verbs(cols=['lemmatize_verbs_test'])
        expected = self.create_dataframe(data={('lemmatize_verbs_test', 'object'): ['the players be tire of study', 'feet and teeth sound simliar', 'it be us connect against the world', 'leave it alone', 'live in the world', 'I be aware that discoveries come very often nowadays']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lemmatize_verbs_all(self):
        df = self.df.copy()
        result = df.cols.lemmatize_verbs(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'], ('japanese name', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lemmatize_verbs_multiple(self):
        df = self.df.copy()
        result = df.cols.lemmatize_verbs(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lemmatize_verbs_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.lemmatize_verbs(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lemmatize_verbs_string(self):
        df = self.df.copy().cols.select(['function(binary)'])
        result = df.cols.lemmatize_verbs(cols=['function(binary)'], output_cols=['function(binary)_2'])
        expected = self.create_dataframe(data={('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('function(binary)_2', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_len(self):
        df = self.create_dataframe(data={('len_test', 'object'): ['      ', '12', '', ' how many characters are in this sentence?', '!@#$%^&*()_>?:}', ' 1     2      3   ']}, force_data_types=True)
        result = df.cols.len(cols=['len_test'])
        expected = self.create_dataframe(data={('len_test', 'int64'): [6, 2, 0, 42, 15, 18]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_len_all(self):
        df = self.df.copy()
        result = df.cols.len(cols='*')
        expected = self.create_dataframe(data={('NullType', 'int64'): [4, 4, 4, 4, 4, 4], ('attributes', 'int64'): [16, 15, 16, 16, 14, 13], ('date arrival', 'int64'): [10, 10, 10, 10, 10, 10], ('function(binary)', 'int64'): [20, 23, 22, 30, 18, 28], ('height(ft)', 'int64'): [5, 4, 4, 4, 3, 5], ('japanese name', 'int64'): [20, 22, 14, 11, 12, 13], ('last date seen', 'int64'): [10, 10, 10, 10, 10, 10], ('last position seen', 'int64'): [20, 20, 21, 21, 4, 4], ('rank', 'int64'): [2, 1, 1, 1, 2, 1], ('Cybertronian', 'int64'): [4, 4, 4, 4, 4, 5], ('Date Type', 'int64'): [10, 10, 10, 10, 10, 10], ('age', 'int64'): [7, 7, 7, 7, 7, 7], ('function', 'int64'): [6, 9, 8, 16, 4, 14], ('names', 'int64'): [7, 12, 9, 4, 8, 13], ('timestamp', 'int64'): [10, 10, 10, 10, 10, 10], ('weight(t)', 'int64'): [3, 3, 3, 3, 3, 3]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_len_multiple(self):
        df = self.df.copy()
        result = df.cols.len(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'int64'): [4, 4, 4, 4, 4, 4], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'int64'): [20, 22, 14, 11, 12, 13], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'int64'): [3, 3, 3, 3, 3, 3]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_len_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.len(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'int64'): [5, 4, 4, 4, 3, 5]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_len_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.len(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'int64'): [7, 12, 9, 4, 8, 13]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lower(self):
        df = self.create_dataframe(data={('lower_test', 'object'): ['ThIs iS A TEST', 'ThIs', 'iS', 'a ', ' TEST', 'this is a test']}, force_data_types=True)
        result = df.cols.lower(cols=['lower_test'])
        expected = self.create_dataframe(data={('lower_test', 'object'): ['this is a test', 'this', 'is', 'a ', ' test', 'this is a test']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lower_all(self):
        df = self.df.copy()
        result = df.cols.lower(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['none', 'none', 'none', 'none', 'none', 'none'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[none, 5700.0]', '[91.44, none]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["bytearray(b'leader')", "bytearray(b'espionage')", "bytearray(b'security')", "bytearray(b'first lieutenant')", "bytearray(b'none')", "bytearray(b'battle station')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'], ('japanese name', 'object'): ["['inochi', 'convoy']", "['bumble', 'goldback']", "['roadbuster']", "['meister']", "['megatron']", "['metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'none', 'none'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['true', 'true', 'true', 'true', 'true', 'false'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['leader', 'espionage', 'security', 'first lieutenant', 'none', 'battle station'], ('names', 'object'): ['optimus', 'bumbl#ebéé  ', 'ironhide&', 'jazz', 'megatron', 'metroplex_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lower_multiple(self):
        df = self.df.copy()
        result = df.cols.lower(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['none', 'none', 'none', 'none', 'none', 'none'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['inochi', 'convoy']", "['bumble', 'goldback']", "['roadbuster']", "['meister']", "['megatron']", "['metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lower_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.lower(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_lower_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.lower(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['optimus', 'bumbl#ebéé  ', 'ironhide&', 'jazz', 'megatron', 'metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_normalize_spaces(self):
        df = self.create_dataframe(data={('normalize_spaces_test', 'object'): ['more       close', '      for  ', 'h   o    w', 'many  words  are in this   sentence', '      ', 'WhatIfThereAreNoSpaces']}, force_data_types=True)
        result = df.cols.normalize_spaces(cols=['normalize_spaces_test'])
        expected = self.create_dataframe(data={('normalize_spaces_test', 'object'): ['more close', ' for ', 'h o w', 'many words are in this sentence', ' ', 'WhatIfThereAreNoSpaces']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_normalize_spaces_all(self):
        df = self.df.copy()
        result = df.cols.normalize_spaces(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'], ('japanese name', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_normalize_spaces_multiple(self):
        df = self.df.copy()
        result = df.cols.normalize_spaces(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_normalize_spaces_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.normalize_spaces(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_normalize_spaces_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.normalize_spaces(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'bumbl#ebéé ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_numbers(self):
        df = self.create_dataframe(data={('remove_numbers_test', 'object'): ['2 plus 2 equals 4', 'bumbl#ebéé   is about 5000000 years old', "these aren't special characters: `~!@#$%^&*()?/\\|", 'why is pi=3.141592... an irrational number?', '3^3=27', "don't @ me"]}, force_data_types=True)
        result = df.cols.remove_numbers(cols=['remove_numbers_test'])
        expected = self.create_dataframe(data={('remove_numbers_test', 'object'): [' plus  equals ', 'bumbl#ebéé   is about  years old', "these aren't special characters: `~!@#$%^&*()?/\\|", 'why is pi=.... an irrational number?', '^=', "don't @ me"]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_numbers_all(self):
        df = self.df.copy()
        result = df.cols.remove_numbers(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[., .]', '[., .]', '[., .]', '[., .]', '[None, .]', '[., None]'], ('date arrival', 'object'): ['//', '//', '//', '//', '//', '//'], ('function(binary)', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"], ('height(ft)', 'object'): ['-.', '.', '.', '.', 'nan', '.'], ('japanese name', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['//', '//', '//', '//', '//', '//'], ('last position seen', 'object'): ['.,-.', '.,-.', '.,-.', '.,-.', 'None', 'None'], ('rank', 'object'): ['', '', '', '', '', ''], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['--', '--', '--', '--', '--', '--'], ('age', 'object'): ['', '', '', '', '', ''], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'object'): ['--', '--', '--', '--', '--', '--'], ('weight(t)', 'object'): ['.', '.', '.', '.', '.', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_numbers_multiple(self):
        df = self.df.copy()
        result = df.cols.remove_numbers(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['.', '.', '.', '.', '.', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_numbers_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.remove_numbers(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-.', '.', '.', '.', 'nan', '.']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_numbers_string(self):
        df = self.df.copy().cols.select(['function(binary)'])
        result = df.cols.remove_numbers(cols=['function(binary)'], output_cols=['function(binary)_2'])
        expected = self.create_dataframe(data={('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('function(binary)_2', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_special_chars(self):
        df = self.create_dataframe(data={('remove_special_chars_test', 'object'): ['2 plus 2 equals 4', 'bumbl#ebéé   is about 5000000 years old', "these aren't special characters: `~!@#$%^&*()?/\\|", 'why is pi=3.141592... an irrational number?', '3^3=27', "don't @ me"]}, force_data_types=True)
        result = df.cols.remove_special_chars(cols=['remove_special_chars_test'])
        expected = self.create_dataframe(data={('remove_special_chars_test', 'object'): ['2 plus 2 equals 4', 'bumblebéé   is about 5000000 years old', 'these arent special characters ', 'why is pi3141592 an irrational number', '3327', 'dont  me']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_special_chars_all(self):
        df = self.df.copy()
        result = df.cols.remove_special_chars(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['85344 43000', '5334 20000', '79248 40000', '39624 18000', 'None 57000', '9144 None'], ('date arrival', 'object'): ['19800410', '19800410', '19800410', '19800410', '19800410', '19800410'], ('function(binary)', 'object'): ['bytearraybLeader', 'bytearraybEspionage', 'bytearraybSecurity', 'bytearraybFirst Lieutenant', 'bytearraybNone', 'bytearraybBattle Station'], ('height(ft)', 'object'): ['280', '170', '260', '130', 'nan', '3000'], ('japanese name', 'object'): ['Inochi Convoy', 'Bumble Goldback', 'Roadbuster', 'Meister', 'Megatron', 'Metroflex'], ('last date seen', 'object'): ['20160910', '20150810', '20140710', '20130610', '20120510', '20110410'], ('last position seen', 'object'): ['1944273599201111', '1064270771612534', '37789563122400356', '33670666117841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['20160910', '20150810', '20140624', '20130624', '20120510', '20110410'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumblebéé  ', 'ironhide', 'Jazz', 'Megatron', 'Metroplex'], ('timestamp', 'object'): ['20140624', '20140624', '20140624', '20140624', '20140624', '20140624'], ('weight(t)', 'object'): ['43', '20', '40', '18', '57', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_special_chars_multiple(self):
        df = self.df.copy()
        result = df.cols.remove_special_chars(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ['Inochi Convoy', 'Bumble Goldback', 'Roadbuster', 'Meister', 'Megatron', 'Metroflex'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['43', '20', '40', '18', '57', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_special_chars_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.remove_special_chars(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['280', '170', '260', '130', 'nan', '3000']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_special_chars_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.remove_special_chars(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'bumblebéé  ', 'ironhide', 'Jazz', 'Megatron', 'Metroplex']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_white_spaces(self):
        df = self.create_dataframe(data={('remove_white_spaces_test', 'object'): ['i   n te  rest i ng ', 'This I s A Test', '     ', 'ThisOneHasNoWhiteSpaces', 'Th is One  Sho   uld Be    AllTogether   ', "L et  ' s G oo o o  o    o"]}, force_data_types=True)
        result = df.cols.remove_white_spaces(cols=['remove_white_spaces_test'])
        expected = self.create_dataframe(data={('remove_white_spaces_test', 'object'): ['interesting', 'ThisIsATest', '', 'ThisOneHasNoWhiteSpaces', 'ThisOneShouldBeAllTogether', "Let'sGoooooo"]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_white_spaces_all(self):
        df = self.df.copy()
        result = df.cols.remove_white_spaces(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[8.5344,4300.0]', '[5.334,2000.0]', '[7.9248,4000.0]', '[3.9624,1800.0]', '[None,5700.0]', '[91.44,None]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'FirstLieutenant')", "bytearray(b'None')", "bytearray(b'BattleStation')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'], ('japanese name', 'object'): ["['Inochi','Convoy']", "['Bumble','Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'FirstLieutenant', 'None', 'BattleStation'], ('names', 'object'): ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_white_spaces_multiple(self):
        df = self.df.copy()
        result = df.cols.remove_white_spaces(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['Inochi','Convoy']", "['Bumble','Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_white_spaces_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.remove_white_spaces(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_remove_white_spaces_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.remove_white_spaces(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reverse(self):
        df = self.create_dataframe(data={('reverse_test', 'object'): ['gnitseretni', 'this Is a TesT', ' ', 'this is another test ', 'tset a si siht', 'reverse esrever']}, force_data_types=True)
        result = df.cols.reverse(cols=['reverse_test'])
        expected = self.create_dataframe(data={('reverse_test', 'object'): ['interesting', 'TseT a sI siht', ' ', ' tset rehtona si siht', 'this is a test', 'reverse esrever']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reverse_all(self):
        df = self.df.copy()
        result = df.cols.reverse(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['enoN', 'enoN', 'enoN', 'enoN', 'enoN', 'enoN'], ('attributes', 'object'): [']0.0034 ,4435.8[', ']0.0002 ,433.5[', ']0.0004 ,8429.7[', ']0.0081 ,4269.3[', ']0.0075 ,enoN[', ']enoN ,44.19['], ('date arrival', 'object'): ['01/40/0891', '01/40/0891', '01/40/0891', '01/40/0891', '01/40/0891', '01/40/0891'], ('function(binary)', 'object'): [")'redaeL'b(yarraetyb", ")'eganoipsE'b(yarraetyb", ")'ytiruceS'b(yarraetyb", ")'tnanetueiL tsriF'b(yarraetyb", ")'enoN'b(yarraetyb", ")'noitatS elttaB'b(yarraetyb"], ('height(ft)', 'object'): ['0.82-', '0.71', '0.62', '0.31', 'nan', '0.003'], ('japanese name', 'object'): ["]'yovnoC' ,'ihconI'[", "]'kcabdloG' ,'elbmuB'[", "]'retsubdaoR'[", "]'retsieM'[", "]'nortageM'[", "]'xelforteM'["], ('last date seen', 'object'): ['01/90/6102', '01/80/5102', '01/70/4102', '01/60/3102', '01/50/2102', '01/40/1102'], ('last position seen', 'object'): ['111102.99-,537244.91', '435216.17-,707246.01', '653004.221-,365987.73', '355148.711-,666076.33', 'enoN', 'enoN'], ('rank', 'object'): ['01', '7', '7', '8', '01', '8'], ('Cybertronian', 'object'): ['eurT', 'eurT', 'eurT', 'eurT', 'eurT', 'eslaF'], ('Date Type', 'object'): ['01-90-6102', '01-80-5102', '42-60-4102', '42-60-3102', '01-50-2102', '01-40-1102'], ('age', 'object'): ['0000005', '0000005', '0000005', '0000005', '0000005', '0000005'], ('function', 'object'): ['redaeL', 'eganoipsE', 'ytiruceS', 'tnanetueiL tsriF', 'enoN', 'noitatS elttaB'], ('names', 'object'): ['sumitpO', '  éébe#lbmub', '&edihnori', 'zzaJ', 'nortageM', '$^)_xelporteM'], ('timestamp', 'object'): ['42-60-4102', '42-60-4102', '42-60-4102', '42-60-4102', '42-60-4102', '42-60-4102'], ('weight(t)', 'object'): ['3.4', '0.2', '0.4', '8.1', '7.5', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reverse_multiple(self):
        df = self.df.copy()
        result = df.cols.reverse(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['enoN', 'enoN', 'enoN', 'enoN', 'enoN', 'enoN'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["]'yovnoC' ,'ihconI'[", "]'kcabdloG' ,'elbmuB'[", "]'retsubdaoR'[", "]'retsieM'[", "]'nortageM'[", "]'xelforteM'["], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['3.4', '0.2', '0.4', '8.1', '7.5', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reverse_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.reverse(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['0.82-', '0.71', '0.62', '0.31', 'nan', '0.003']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reverse_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.reverse(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['sumitpO', '  éébe#lbmub', '&edihnori', 'zzaJ', 'nortageM', '$^)_xelporteM']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_title(self):
        df = self.create_dataframe(data={('title_test', 'object'): ['ThIs iS A TEST', 'ThIs', 'iS', 'a ', ' TEST', 'this is a test']}, force_data_types=True)
        result = df.cols.title(cols=['title_test'])
        expected = self.create_dataframe(data={('title_test', 'object'): ['This Is A Test', 'This', 'Is', 'A ', ' Test', 'This Is A Test']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_title_all(self):
        df = self.df.copy()
        result = df.cols.title(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["Bytearray(B'Leader')", "Bytearray(B'Espionage')", "Bytearray(B'Security')", "Bytearray(B'First Lieutenant')", "Bytearray(B'None')", "Bytearray(B'Battle Station')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0'], ('japanese name', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'Bumbl#Ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_title_multiple(self):
        df = self.df.copy()
        result = df.cols.title(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_title_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.title(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_title_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.title(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'Bumbl#Ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_trim(self):
        df = self.create_dataframe(data={('trim_test', 'object'): ['ThIs iS A TEST', 'foo', '     bar', 'baz      ', '      zoo     ', '   t e   s   t   ']}, force_data_types=True)
        result = df.cols.trim(cols=['trim_test'])
        expected = self.create_dataframe(data={('trim_test', 'object'): ['ThIs iS A TEST', 'foo', 'bar', 'baz', 'zoo', 't e   s   t']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_trim_all(self):
        df = self.df.copy()
        result = df.cols.trim(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'], ('japanese name', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_trim_multiple(self):
        df = self.df.copy()
        result = df.cols.trim(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_trim_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.trim(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_trim_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.trim(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_upper(self):
        df = self.create_dataframe(data={('upper_test', 'object'): ['ThIs iS A TEST', 'ThIs', 'iS', 'a ', ' TEST', 'this is a test']}, force_data_types=True)
        result = df.cols.upper(cols=['upper_test'])
        expected = self.create_dataframe(data={('upper_test', 'object'): ['THIS IS A TEST', 'THIS', 'IS', 'A ', ' TEST', 'THIS IS A TEST']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_upper_all(self):
        df = self.df.copy()
        result = df.cols.upper(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): ['NONE', 'NONE', 'NONE', 'NONE', 'NONE', 'NONE'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[NONE, 5700.0]', '[91.44, NONE]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["BYTEARRAY(B'LEADER')", "BYTEARRAY(B'ESPIONAGE')", "BYTEARRAY(B'SECURITY')", "BYTEARRAY(B'FIRST LIEUTENANT')", "BYTEARRAY(B'NONE')", "BYTEARRAY(B'BATTLE STATION')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'NAN', '300.0'], ('japanese name', 'object'): ["['INOCHI', 'CONVOY']", "['BUMBLE', 'GOLDBACK']", "['ROADBUSTER']", "['MEISTER']", "['MEGATRON']", "['METROFLEX']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'NONE', 'NONE'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'FALSE'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['LEADER', 'ESPIONAGE', 'SECURITY', 'FIRST LIEUTENANT', 'NONE', 'BATTLE STATION'], ('names', 'object'): ['OPTIMUS', 'BUMBL#EBÉÉ  ', 'IRONHIDE&', 'JAZZ', 'MEGATRON', 'METROPLEX_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'NAN']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_upper_multiple(self):
        df = self.df.copy()
        result = df.cols.upper(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['NONE', 'NONE', 'NONE', 'NONE', 'NONE', 'NONE'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ["['INOCHI', 'CONVOY']", "['BUMBLE', 'GOLDBACK']", "['ROADBUSTER']", "['MEISTER']", "['MEGATRON']", "['METROFLEX']"], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'NAN']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_upper_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.upper(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'NAN', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_upper_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.upper(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['OPTIMUS', 'BUMBL#EBÉÉ  ', 'IRONHIDE&', 'JAZZ', 'MEGATRON', 'METROPLEX_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_count(self):
        df = self.create_dataframe(data={('word_count_test', 'object'): ['THis iS a TEST', 'for', ' how ', 'many words are in this sentence', '      ', '12']}, force_data_types=True)
        result = df.cols.word_count(cols=['word_count_test'])
        expected = self.create_dataframe(data={('word_count_test', 'int64'): [4, 1, 1, 6, 0, 1]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_count_all(self):
        df = self.df.copy()
        result = df.cols.word_count(cols='*')
        expected = self.create_dataframe(data={('NullType', 'int64'): [1, 1, 1, 1, 1, 1], ('attributes', 'int64'): [5, 5, 5, 5, 5, 5], ('date arrival', 'int64'): [1, 1, 1, 1, 1, 1], ('function(binary)', 'int64'): [5, 5, 5, 6, 5, 6], ('height(ft)', 'int64'): [1, 1, 1, 1, 1, 1], ('japanese name', 'int64'): [7, 7, 4, 4, 4, 4], ('last date seen', 'int64'): [1, 1, 1, 1, 1, 1], ('last position seen', 'int64'): [3, 3, 3, 3, 1, 1], ('rank', 'int64'): [1, 1, 1, 1, 1, 1], ('Cybertronian', 'int64'): [1, 1, 1, 1, 1, 1], ('Date Type', 'int64'): [1, 1, 1, 1, 1, 1], ('age', 'int64'): [1, 1, 1, 1, 1, 1], ('function', 'int64'): [1, 1, 1, 2, 1, 2], ('names', 'int64'): [1, 3, 2, 1, 1, 4], ('timestamp', 'int64'): [1, 1, 1, 1, 1, 1], ('weight(t)', 'int64'): [1, 1, 1, 1, 1, 1]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_count_multiple(self):
        df = self.df.copy()
        result = df.cols.word_count(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'int64'): [1, 1, 1, 1, 1, 1], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'int64'): [7, 7, 4, 4, 4, 4], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'int64'): [1, 1, 1, 1, 1, 1]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_count_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.word_count(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'int64'): [1, 1, 1, 1, 1, 1]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_count_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.word_count(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'int64'): [1, 3, 2, 1, 1, 4]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_tokenize(self):
        df = self.create_dataframe(data={('word_tokenize_test', 'object'): ['THis iS a TEST', 'for', ' how ', 'many words are in this sentence', '      ', '12']}, force_data_types=True)
        result = df.cols.word_tokenize(cols=['word_tokenize_test'])
        expected = self.create_dataframe(data={('word_tokenize_test', 'object'): [['THis', 'iS', 'a', 'TEST'], ['for'], ['how'], ['many', 'words', 'are', 'in', 'this', 'sentence'], [], ['12']]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_tokenize_all(self):
        df = self.df.copy()
        result = df.cols.word_tokenize(cols='*')
        expected = self.create_dataframe(data={('NullType', 'object'): [['None'], ['None'], ['None'], ['None'], ['None'], ['None']], ('attributes', 'object'): [['[', '8.5344', ',', '4300.0', ']'], ['[', '5.334', ',', '2000.0', ']'], ['[', '7.9248', ',', '4000.0', ']'], ['[', '3.9624', ',', '1800.0', ']'], ['[', 'None', ',', '5700.0', ']'], ['[', '91.44', ',', 'None', ']']], ('date arrival', 'object'): [['1980/04/10'], ['1980/04/10'], ['1980/04/10'], ['1980/04/10'], ['1980/04/10'], ['1980/04/10']], ('function(binary)', 'object'): [['bytearray', '(', "b'Leader", "'", ')'], ['bytearray', '(', "b'Espionage", "'", ')'], ['bytearray', '(', "b'Security", "'", ')'], ['bytearray', '(', "b'First", 'Lieutenant', "'", ')'], ['bytearray', '(', "b'None", "'", ')'], ['bytearray', '(', "b'Battle", 'Station', "'", ')']], ('height(ft)', 'object'): [['-28.0'], ['17.0'], ['26.0'], ['13.0'], ['nan'], ['300.0']], ('japanese name', 'object'): [['[', "'Inochi", "'", ',', "'Convoy", "'", ']'], ['[', "'Bumble", "'", ',', "'Goldback", "'", ']'], ['[', "'Roadbuster", "'", ']'], ['[', "'Meister", "'", ']'], ['[', "'Megatron", "'", ']'], ['[', "'Metroflex", "'", ']']], ('last date seen', 'object'): [['2016/09/10'], ['2015/08/10'], ['2014/07/10'], ['2013/06/10'], ['2012/05/10'], ['2011/04/10']], ('last position seen', 'object'): [['19.442735', ',', '-99.201111'], ['10.642707', ',', '-71.612534'], ['37.789563', ',', '-122.400356'], ['33.670666', ',', '-117.841553'], ['None'], ['None']], ('rank', 'object'): [['10'], ['7'], ['7'], ['8'], ['10'], ['8']], ('Cybertronian', 'object'): [['True'], ['True'], ['True'], ['True'], ['True'], ['False']], ('Date Type', 'object'): [['2016-09-10'], ['2015-08-10'], ['2014-06-24'], ['2013-06-24'], ['2012-05-10'], ['2011-04-10']], ('age', 'object'): [['5000000'], ['5000000'], ['5000000'], ['5000000'], ['5000000'], ['5000000']], ('function', 'object'): [['Leader'], ['Espionage'], ['Security'], ['First', 'Lieutenant'], ['None'], ['Battle', 'Station']], ('names', 'object'): [['Optimus'], ['bumbl', '#', 'ebéé'], ['ironhide', '&'], ['Jazz'], ['Megatron'], ['Metroplex_', ')', '^', '$']], ('timestamp', 'object'): [['2014-06-24'], ['2014-06-24'], ['2014-06-24'], ['2014-06-24'], ['2014-06-24'], ['2014-06-24']], ('weight(t)', 'object'): [['4.3'], ['2.0'], ['4.0'], ['1.8'], ['5.7'], ['nan']]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_tokenize_multiple(self):
        df = self.df.copy()
        result = df.cols.word_tokenize(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): [['None'], ['None'], ['None'], ['None'], ['None'], ['None']], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): [['[', "'Inochi", "'", ',', "'Convoy", "'", ']'], ['[', "'Bumble", "'", ',', "'Goldback", "'", ']'], ['[', "'Roadbuster", "'", ']'], ['[', "'Meister", "'", ']'], ['[', "'Megatron", "'", ']'], ['[', "'Metroflex", "'", ']']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): [['4.3'], ['2.0'], ['4.0'], ['1.8'], ['5.7'], ['nan']]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_tokenize_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.word_tokenize(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'object'): [['-28.0'], ['17.0'], ['26.0'], ['13.0'], ['nan'], ['300.0']]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_word_tokenize_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.word_tokenize(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): [['Optimus'], ['bumbl', '#', 'ebéé'], ['ironhide', '&'], ['Jazz'], ['Megatron'], ['Metroplex_', ')', '^', '$']]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))


class TestStringDask(TestStringPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestStringPartitionDask(TestStringPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringCUDF(TestStringPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringDC(TestStringPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringPartitionDC(TestStringPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringSpark(TestStringPandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStringVaex(TestStringPandas):
        config = {'engine': 'vaex'}
