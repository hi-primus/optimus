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


class TestReplacePandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Cybertronian', 'object'): ['False', 'True + 1', 1, None, True, False], ('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None

    def test_cols_replace_list_chars(self):
        df = self.df.copy().cols.select(['function'])
        result = df.cols.replace(cols=['function'], search_by='chars', search=['True', 'False'], replace_by=['Maybe', 'Unlikely'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_full(self):
        df = self.df.copy().cols.select(['function'])
        result = df.cols.replace(cols=['function'], search_by='full', search=['True', 'False'], replace_by=['Maybe', 'Unlikely'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_value2_chars(self):
        df = self.df.copy().cols.select('function')
        result = df.cols.replace(cols='function', search_by='chars', search=['leader', 'espionage', 'security'], replace_by='this is a test', ignore_case=True)
        expected = self.create_dataframe(data={('function', 'object'): ['this is a test', 'this is a test', 'this is a test', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_value2_full(self):
        df = self.df.copy().cols.select('function')
        result = df.cols.replace(cols='function', search_by='full', search=['leader', 'espionage', 'security'], replace_by='this is a test', ignore_case=True)
        expected = self.create_dataframe(data={('function', 'object'): ['this is a test', 'this is a test', 'this is a test', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_value2_values(self):
        df = self.df.copy().cols.select('function')
        result = df.cols.replace(cols='function', search_by='values', search=['leader', 'espionage', 'security'], replace_by='this is a test', ignore_case=True)
        expected = self.create_dataframe(data={('function', 'object'): ['this is a test', 'this is a test', 'this is a test', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_value2_words(self):
        df = self.df.copy().cols.select('function')
        result = df.cols.replace(cols='function', search_by='words', search=['leader', 'espionage', 'security'], replace_by='this is a test', ignore_case=True)
        expected = self.create_dataframe(data={('function', 'object'): ['this is a test', 'this is a test', 'this is a test', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_value_chars(self):
        df = self.df.copy().cols.select('function')
        result = df.cols.replace(cols='function', search_by='chars', search=['Leader', 'Firs', 'None', 'Secu'], replace_by='MATCH')
        expected = self.create_dataframe(data={('function', 'object'): ['MATCH', 'Espionage', 'MATCHrity', 'MATCHt Lieutenant', 'MATCH', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_value_full(self):
        df = self.df.copy().cols.select('function')
        result = df.cols.replace(cols='function', search_by='full', search=['Leader', 'Firs', 'None', 'Secu'], replace_by='MATCH')
        expected = self.create_dataframe(data={('function', 'object'): ['MATCH', 'Espionage', 'Security', 'First Lieutenant', 'MATCH', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_value_values(self):
        df = self.df.copy().cols.select('function')
        result = df.cols.replace(cols='function', search_by='values', search=['Leader', 'Firs', 'None', 'Secu'], replace_by='MATCH')
        expected = self.create_dataframe(data={('function', 'object'): ['MATCH', 'Espionage', 'Security', 'First Lieutenant', 'MATCH', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_value_words(self):
        df = self.df.copy().cols.select('function')
        result = df.cols.replace(cols='function', search_by='words', search=['Leader', 'Firs', 'None', 'Secu'], replace_by='MATCH')
        expected = self.create_dataframe(data={('function', 'object'): ['MATCH', 'Espionage', 'Security', 'First Lieutenant', 'MATCH', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_values(self):
        df = self.df.copy().cols.select(['function'])
        result = df.cols.replace(cols=['function'], search_by='values', search=['True', 'False'], replace_by=['Maybe', 'Unlikely'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_list_words(self):
        df = self.df.copy().cols.select(['function'])
        result = df.cols.replace(cols=['function'], search_by='words', search=['True', 'False'], replace_by=['Maybe', 'Unlikely'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_lists_list_chars(self):
        df = self.df.copy().cols.select(['function', 'japanese name'])
        result = df.cols.replace(cols=['function', 'japanese name'], search_by='chars', search=['atro', 'nochi'], replace_by=['Inochi', 'Megatron'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('japanese name', 'object'): ["['IMegatron', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['MegInochin']", "['Metroflex']"]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_lists_list_full(self):
        df = self.df.copy().cols.select(['function', 'Code', 'Cybertronian'])
        result = df.cols.replace(cols=['function', 'Code', 'Cybertronian'], search_by='full', search=['123', '456', '123A'], replace_by=['N/A', 'Number', 'String'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('Code', 'object'): ['String', 'Number', 456, 'e', None, '{code}'], ('Cybertronian', 'object'): ['False', 'True + 1', 1, None, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_lists_list_values(self):
        df = self.df.copy().cols.select(['function', 'japanese name'])
        result = df.cols.replace(cols=['function', 'japanese name'], search_by='values', search=['atro', 'nochi'], replace_by=['Inochi', 'Megatron'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_lists_list_words(self):
        df = self.df.copy().cols.select(['function', 'japanese name'])
        result = df.cols.replace(cols=['function', 'japanese name'], search_by='words', search=['atro', 'Megatron', 'nochi', 'Inochi'], replace_by=['--', 'a-match', '--', 'other-match'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('japanese name', 'object'): ["['other-match', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['a-match']", "['Metroflex']"]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_list_list_chars(self):
        df = self.df.copy().cols.select(['function'])
        result = df.cols.replace_regex(cols=['function'], search_by='chars', search=['....', '.....', '......'], replace_by=['4c', '5c', '6c'])
        expected = self.create_dataframe(data={('function', 'object'): ['4cer', '5c', '4c4c', '5cc4c', '4c', '5ccon']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_list_list_full(self):
        df = self.df.copy().cols.select(['function'])
        result = df.cols.replace_regex(cols=['function'], search_by='full', search=['....', '.....', '......'], replace_by=['4c', '5c', '6c'])
        expected = self.create_dataframe(data={('function', 'object'): ['6c', 'Espionage', 'Security', 'First Lieutenant', '4c', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_list_list_values(self):
        df = self.df.copy().cols.select(['function'])
        result = df.cols.replace_regex(cols=['function'], search_by='values', search=['....', '.....', '......'], replace_by=['4c', '5c', '6c'])
        expected = self.create_dataframe(data={('function', 'object'): ['6c', 'Espionage', 'Security', 'First Lieutenant', '4c', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_list_list_words(self):
        df = self.df.copy().cols.select(['function'])
        result = df.cols.replace_regex(cols=['function'], search_by='words', search=['....', '.....', '......'], replace_by=['4c', '5c', '6c'])
        expected = self.create_dataframe(data={('function', 'object'): ['6c', 'Espionage', 'Security', '5c Lieutenant', '4c', '6c Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_list_value_chars(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace_regex(cols='names', search_by='chars', search=['^a', '^e', '^i', '^o', '^u'], replace_by='starts with a vowel', ignore_case=True)
        expected = self.create_dataframe(data={('names', 'object'): ['starts with a vowelptimus', 'bumbl#ebéé  ', 'starts with a vowelronhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_list_value_full(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace_regex(cols='names', search_by='full', search=['^a', '^e', '^i', '^o', '^u'], replace_by='starts with a vowel', ignore_case=True)
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_list_value_words(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace_regex(cols='names', search_by='words', search=['^a', '^e', '^i', '^o', '^u'], replace_by='starts with a vowel', ignore_case=True)
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_lists_list_chars(self):
        df = self.df.copy().cols.select(['function', 'Code', 'Cybertronian'])
        result = df.cols.replace_regex(cols=['function', 'Code', 'Cybertronian'], search_by='chars', search=[['\\d'], ['.*Tru.*', '.*Fal.*']], replace_by=['has a number', 'has boolean'])
        expected = self.create_dataframe(data={('function', 'object'): ['Lehas booleanhhas booleanshas booleanhas booleanhas booleannhas booleanmbehas booleanehas boolean', 'Espionhas booleange', 'Sechas booleanhas booleanity', 'has booleanihas booleansthas booleanLiehas booleantenhas booleannt', 'None', 'Bhas booleantthas booleanehas booleanSthas booleantion'], ('Code', 'object'): ['123A', '456', '456', 'e', 'None', '{cohas a numbere}'], ('Cybertronian', 'object'): ['has booleanhas booleanhas booleanse', 'has booleanhas booleanhas booleanehas boolean+has boolean1', '1', 'None', 'has booleanhas booleanhas booleane', 'has booleanhas booleanhas booleanse']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_lists_list_full(self):
        df = self.df.copy().cols.select(['function', 'Code', 'Cybertronian'])
        result = df.cols.replace_regex(cols=['function', 'Code', 'Cybertronian'], search_by='full', search=[['\\d', 'e'], ['.*True.*', '.*False.*']], replace_by=['is a number', 'is boolean'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('Code', 'object'): ['123A', '456', 456, 'is a number', None, '{code}'], ('Cybertronian', 'object'): ['False', 'True + 1', 1, None, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_lists_list_values(self):
        df = self.df.copy().cols.select(['function', 'Code', 'Cybertronian'])
        result = df.cols.replace_regex(cols=['function', 'Code', 'Cybertronian'], search_by='values', search=[['\\d'], ['.*True.*', '.*False.*']], replace_by=['is a number', 'is boolean'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('Code', 'object'): ['123A', '456', 456, 'is boolean', None, '{code}'], ('Cybertronian', 'object'): ['False', 'True + 1', 1, None, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_lists_list_words(self):
        df = self.df.copy().cols.select(['function', 'Code', 'Cybertronian'])
        result = df.cols.replace_regex(cols=['function', 'Code', 'Cybertronian'], search_by='words', search=[['\\d'], ['.*True.*', '.*False.*']], replace_by=['is a number', 'is boolean'])
        expected = self.create_dataframe(data={('function', 'object'): ['Leader', 'Espionage', 'Security', 'Firstis booleanLieutenant', 'None', 'Battleis booleanStation'], ('Code', 'object'): ['123A', '456', '456', 'is boolean', 'None', '{code}'], ('Cybertronian', 'object'): ['False', 'True + 1', '1', 'None', 'True', 'False']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_value_chars(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace_regex(cols='names', search_by='chars', search='.*atro.*', replace_by='must be Megatron')
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'must be Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_value_full(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace_regex(cols='names', search_by='full', search='.*atro.*', replace_by='must be Megatron')
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'must be Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_value_values(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace_regex(cols='names', search_by='values', search='.*atro.*', replace_by='must be Megatron')
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'must be Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_regex_value_words(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace_regex(cols='names', search_by='words', search='.*atro.*', replace_by='must be Megatron')
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'must be Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_types_chars(self):
        df = self.df.copy().cols.select('*')
        result = df.cols.replace(cols='*', search_by='chars', search=['at', 'an'], replace_by='this is a test')
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megthis is a testron', 'Metroplex_)^$'], ('japanese name', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megthis is a testron']", "['Metroflex']"], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenthis is a testt', 'None', 'Bthis is a testtle Stthis is a testion'], ('Code', 'object'): ['123A', '456', '456', 'e', 'None', '{code}'], ('Cybertronian', 'object'): ['False', 'True + 1', '1', 'None', 'True', 'False'], ('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenthis is a testt')", "bytearray(b'None')", "bytearray(b'Bthis is a testtle Stthis is a testion')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nthis is a test', '300.0'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nthis is a test']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_types_full(self):
        df = self.df.copy().cols.select('*')
        result = df.cols.replace(cols='*', search_by='full', search=['at', 'an'], replace_by='this is a test')
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Cybertronian', 'object'): ['False', 'True + 1', 1, None, True, False], ('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_types_values(self):
        df = self.df.copy().cols.select('*')
        result = df.cols.replace(cols='*', search_by='values', search=['at', 'an'], replace_by='this is a test')
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Cybertronian', 'object'): ['False', 'True + 1', 1, None, True, False], ('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_types_words(self):
        df = self.df.copy().cols.select('*')
        result = df.cols.replace(cols='*', search_by='words', search=['at', 'an'], replace_by='this is a test')
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('japanese name', 'object'): ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('Code', 'object'): ['123A', '456', '456', 'e', 'None', '{code}'], ('Cybertronian', 'object'): ['False', 'True + 1', '1', 'None', 'True', 'False'], ('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_value_chars(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace(cols='names', search_by='chars', search='Optimus', replace_by='optimus prime')
        expected = self.create_dataframe(data={('names', 'object'): ['optimus prime', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_value_full(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace(cols='names', search_by='full', search='Optimus', replace_by='optimus prime')
        expected = self.create_dataframe(data={('names', 'object'): ['optimus prime', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_value_values(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace(cols='names', search_by='values', search='Optimus', replace_by='optimus prime')
        expected = self.create_dataframe(data={('names', 'object'): ['optimus prime', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_replace_value_words(self):
        df = self.df.copy().cols.select('names')
        result = df.cols.replace(cols='names', search_by='words', search='Optimus', replace_by='optimus prime')
        expected = self.create_dataframe(data={('names', 'object'): ['optimus prime', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))


class TestReplaceDask(TestReplacePandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestReplacePartitionDask(TestReplacePandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestReplaceCUDF(TestReplacePandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestReplaceDC(TestReplacePandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestReplacePartitionDC(TestReplacePandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestReplaceSpark(TestReplacePandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestReplaceVaex(TestReplacePandas):
        config = {'engine': 'vaex'}
