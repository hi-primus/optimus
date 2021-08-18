from optimus.tests.base import TestBase
import datetime
Timestamp = lambda t: datetime.datetime.strptime(t,"%Y-%m-%d %H:%M:%S")
nan = float("nan")
inf = float("inf")
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort, df_dicts_equal

class TestReplacePandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None
    
    def test_cols_replace_list_chars(self):
        df = self.df.cols.select(['Cybertronian'])
        result = df.cols.replace(cols=['Cybertronian'],output_cols=['out_test'],search_by='chars',search=['True', 'False'],replace_by=['Maybe', 'Unlikely'])
        expected = self.create_dataframe(dict={('Cybertronian', 'bool'): [True, True, True, True, True, False], ('out_test', 'object'): ['Maybe', 'Maybe', 'Maybe', 'Maybe', 'Maybe', 'Unlikely']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_full(self):
        df = self.df.cols.select(['Cybertronian'])
        result = df.cols.replace(cols=['Cybertronian'],output_cols=['out_test'],search_by='full',search=['True', 'False'],replace_by=['Maybe', 'Unlikely'])
        expected = self.create_dataframe(dict={('Cybertronian', 'bool'): [True, True, True, True, True, False], ('out_test', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_value2_chars(self):
        df = self.df.cols.select('function')
        result = df.cols.replace(cols='function',search_by='chars',search=['leader', 'espionage', 'security'],replace_by='this is a test',ignore_case=True)
        expected = self.create_dataframe(dict={('function', 'object'): ['this is a test', 'this is a test', 'this is a test', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_value2_full(self):
        df = self.df.cols.select('function')
        result = df.cols.replace(cols='function',search_by='full',search=['leader', 'espionage', 'security'],replace_by='this is a test',ignore_case=True)
        expected = self.create_dataframe(dict={('function', 'object'): ['this is a test', 'this is a test', 'this is a test', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_value2_values(self):
        df = self.df.cols.select('function')
        result = df.cols.replace(cols='function',search_by='values',search=['leader', 'espionage', 'security'],replace_by='this is a test',ignore_case=True)
        expected = self.create_dataframe(dict={('function', 'object'): ['this is a test', 'this is a test', 'this is a test', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_value2_words(self):
        df = self.df.cols.select('function')
        result = df.cols.replace(cols='function',search_by='words',search=['leader', 'espionage', 'security'],replace_by='this is a test',ignore_case=True)
        expected = self.create_dataframe(dict={('function', 'object'): ['this is a test', 'this is a test', 'this is a test', 'First Lieutenant', 'None', 'Battle Station']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_value_chars(self):
        df = self.df.cols.select('rank')
        result = df.cols.replace(cols='rank',search_by='chars',search=[10, 8, 6, 4, 2],replace_by='this is an even number')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_value_full(self):
        df = self.df.cols.select('rank')
        result = df.cols.replace(cols='rank',search_by='full',search=[10, 8, 6, 4, 2],replace_by='this is an even number')
        expected = self.create_dataframe(dict={('rank', 'object'): ['this is an even number', 7, 7, 'this is an even number', 'this is an even number', 'this is an even number']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_value_values(self):
        df = self.df.cols.select('rank')
        result = df.cols.replace(cols='rank',search_by='values',search=[10, 8, 6, 4, 2],replace_by='this is an even number')
        expected = self.create_dataframe(dict={('rank', 'object'): ['this is an even number', 7, 7, 'this is an even number', 'this is an even number', 'this is an even number']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_value_words(self):
        df = self.df.cols.select('rank')
        result = df.cols.replace(cols='rank',search_by='words',search=[10, 8, 6, 4, 2],replace_by='this is an even number')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_values(self):
        df = self.df.cols.select(['Cybertronian'])
        result = df.cols.replace(cols=['Cybertronian'],output_cols=['out_test'],search_by='values',search=['True', 'False'],replace_by=['Maybe', 'Unlikely'])
        expected = self.create_dataframe(dict={('Cybertronian', 'bool'): [True, True, True, True, True, False], ('out_test', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_list_words(self):
        df = self.df.cols.select(['Cybertronian'])
        result = df.cols.replace(cols=['Cybertronian'],output_cols=['out_test'],search_by='words',search=['True', 'False'],replace_by=['Maybe', 'Unlikely'])
        expected = self.create_dataframe(dict={('Cybertronian', 'bool'): [True, True, True, True, True, False], ('out_test', 'object'): ['Maybe', 'Maybe', 'Maybe', 'Maybe', 'Maybe', 'Unlikely']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_lists_list_chars(self):
        df = self.df.cols.select(['attributes', 'japanese name'])
        result = df.cols.replace(cols=['attributes', 'japanese name'],output_cols=['out_tes1', 'out_test2'],search_by='chars',search=[['Inochi', 'Convoy'], [91.44, None], ['Megatron']],replace_by=['Inochi', 91.44, 'Megatron'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_lists_list_full(self):
        df = self.df.cols.select(['attributes', 'japanese name'])
        result = df.cols.replace(cols=['attributes', 'japanese name'],output_cols=['out_tes1', 'out_test2'],search_by='full',search=[['Inochi', 'Convoy'], [91.44, None], ['Megatron']],replace_by=['Inochi', 91.44, 'Megatron'])
        expected = self.create_dataframe(dict={('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('out_tes1', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('out_test2', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_lists_list_values(self):
        df = self.df.cols.select(['attributes', 'japanese name'])
        result = df.cols.replace(cols=['attributes', 'japanese name'],output_cols=['out_tes1', 'out_test2'],search_by='values',search=[['Inochi', 'Convoy'], [91.44, None], ['Megatron']],replace_by=['Inochi', 91.44, 'Megatron'])
        expected = self.create_dataframe(dict={('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('out_tes1', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('out_test2', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_lists_list_words(self):
        df = self.df.cols.select(['attributes', 'japanese name'])
        result = df.cols.replace(cols=['attributes', 'japanese name'],output_cols=['out_tes1', 'out_test2'],search_by='words',search=[['Inochi', 'Convoy'], [91.44, None], ['Megatron']],replace_by=['Inochi', 91.44, 'Megatron'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_list_chars(self):
        df = self.df.cols.select(['height(ft)', 'function'])
        result = df.cols.replace_regex(cols=['height(ft)', 'function'],output_cols=['out_test'],search_by='chars',search=['....', '.....', '......'],replace_by=['a four character word', 'a five character word', 'a six character word'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_list_full(self):
        df = self.df.cols.select(['height(ft)', 'function'])
        result = df.cols.replace_regex(cols=['height(ft)', 'function'],output_cols=['out_test'],search_by='full',search=['....', '.....', '......'],replace_by=['a four character word', 'a five character word', 'a six character word'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_list_values(self):
        df = self.df.cols.select(['height(ft)', 'function'])
        result = df.cols.replace_regex(cols=['height(ft)', 'function'],output_cols=['out_test'],search_by='values',search=['....', '.....', '......'],replace_by=['a four character word', 'a five character word', 'a six character word'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_list_words(self):
        df = self.df.cols.select(['height(ft)', 'function'])
        result = df.cols.replace_regex(cols=['height(ft)', 'function'],output_cols=['out_test'],search_by='words',search=['....', '.....', '......'],replace_by=['a four character word', 'a five character word', 'a six character word'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_value2_chars(self):
        df = self.df.cols.select('names')
        result = df.cols.replace_regex(cols='names',search_by='chars',search=['^a', '^e', '^i', '^o', '^u'],replace_by='starts with a vowel',ignore_case=True)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_value2_full(self):
        df = self.df.cols.select('names')
        result = df.cols.replace_regex(cols='names',search_by='full',search=['^a', '^e', '^i', '^o', '^u'],replace_by='starts with a vowel',ignore_case=True)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_value2_words(self):
        df = self.df.cols.select('names')
        result = df.cols.replace_regex(cols='names',search_by='words',search=['^a', '^e', '^i', '^o', '^u'],replace_by='starts with a vowel',ignore_case=True)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_value_chars(self):
        df = self.df.cols.select('*')
        result = df.cols.replace_regex(cols='*',search_by='chars',search=['^a', '^e', '^i', '^o', '^u'],replace_by='starts with a vowel')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_value_full(self):
        df = self.df.cols.select('*')
        result = df.cols.replace_regex(cols='*',search_by='full',search=['^a', '^e', '^i', '^o', '^u'],replace_by='starts with a vowel')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_value_values(self):
        df = self.df.cols.select('*')
        result = df.cols.replace_regex(cols='*',search_by='values',search=['^a', '^e', '^i', '^o', '^u'],replace_by='starts with a vowel')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_list_value_words(self):
        df = self.df.cols.select('*')
        result = df.cols.replace_regex(cols='*',search_by='words',search=['^a', '^e', '^i', '^o', '^u'],replace_by='starts with a vowel')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_lists_list_chars(self):
        df = self.df.cols.select(['NullType', 'weight(t)', 'japanese name'])
        result = df.cols.replace_regex(cols=['NullType', 'weight(t)', 'japanese name'],search_by='chars',search=[['1$', '2$', '3$', '4$', '5$', '6$', '7$', '8$', '9$', '0$'],
 ['.*True.*', '.*False.*']],replace_by=['is a number', 'is boolean'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_lists_list_full(self):
        df = self.df.cols.select(['NullType', 'weight(t)', 'japanese name'])
        result = df.cols.replace_regex(cols=['NullType', 'weight(t)', 'japanese name'],search_by='full',search=[['1$', '2$', '3$', '4$', '5$', '6$', '7$', '8$', '9$', '0$'],
 ['.*True.*', '.*False.*']],replace_by=['is a number', 'is boolean'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_lists_list_values(self):
        df = self.df.cols.select(['NullType', 'weight(t)', 'japanese name'])
        result = df.cols.replace_regex(cols=['NullType', 'weight(t)', 'japanese name'],search_by='values',search=[['1$', '2$', '3$', '4$', '5$', '6$', '7$', '8$', '9$', '0$'],
 ['.*True.*', '.*False.*']],replace_by=['is a number', 'is boolean'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_lists_list_words(self):
        df = self.df.cols.select(['NullType', 'weight(t)', 'japanese name'])
        result = df.cols.replace_regex(cols=['NullType', 'weight(t)', 'japanese name'],search_by='words',search=[['1$', '2$', '3$', '4$', '5$', '6$', '7$', '8$', '9$', '0$'],
 ['.*True.*', '.*False.*']],replace_by=['is a number', 'is boolean'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_value_chars(self):
        df = self.df.cols.select('names')
        result = df.cols.replace_regex(cols='names',search_by='chars',search='.*atro.*',replace_by='must be Megatron')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_value_full(self):
        df = self.df.cols.select('names')
        result = df.cols.replace_regex(cols='names',search_by='full',search='.*atro.*',replace_by='must be Megatron')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_value_values(self):
        df = self.df.cols.select('names')
        result = df.cols.replace_regex(cols='names',search_by='values',search='.*atro.*',replace_by='must be Megatron')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_regex_value_words(self):
        df = self.df.cols.select('names')
        result = df.cols.replace_regex(cols='names',search_by='words',search='.*atro.*',replace_by='must be Megatron')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_types_chars(self):
        df = self.df.cols.select('*')
        result = df.cols.replace(cols='*',search_by='chars',search=[None, 'True', bytearray(b'Leader'), [5.334, 2000.0],
 datetime.datetime(2016, 9, 10, 0, 0), 5000000, '1980/04/10'],replace_by='this is a test')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_types_full(self):
        df = self.df.cols.select('*')
        result = df.cols.replace(cols='*',search_by='full',search=[None, 'True', bytearray(b'Leader'), [5.334, 2000.0],
 datetime.datetime(2016, 9, 10, 0, 0), 5000000, '1980/04/10'],replace_by='this is a test')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_types_values(self):
        df = self.df.cols.select('*')
        result = df.cols.replace(cols='*',search_by='values',search=[None, 'True', bytearray(b'Leader'), [5.334, 2000.0],
 datetime.datetime(2016, 9, 10, 0, 0), 5000000, '1980/04/10'],replace_by='this is a test')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_types_words(self):
        df = self.df.cols.select('*')
        result = df.cols.replace(cols='*',search_by='words',search=[None, 'True', bytearray(b'Leader'), [5.334, 2000.0],
 datetime.datetime(2016, 9, 10, 0, 0), 5000000, '1980/04/10'],replace_by='this is a test')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_value_chars(self):
        df = self.df.cols.select('names')
        result = df.cols.replace(cols='names',search_by='chars',search='Optimus',replace_by='optimus prime')
        expected = self.create_dataframe(dict={('names', 'object'): ['optimus prime', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_value_full(self):
        df = self.df.cols.select('names')
        result = df.cols.replace(cols='names',search_by='full',search='Optimus',replace_by='optimus prime')
        expected = self.create_dataframe(dict={('names', 'object'): ['optimus prime', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_value_values(self):
        df = self.df.cols.select('names')
        result = df.cols.replace(cols='names',search_by='values',search='Optimus',replace_by='optimus prime')
        expected = self.create_dataframe(dict={('names', 'object'): ['optimus prime', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_replace_value_words(self):
        df = self.df.cols.select('names')
        result = df.cols.replace(cols='names',search_by='words',search='Optimus',replace_by='optimus prime')
        expected = self.create_dataframe(dict={('names', 'object'): ['optimus prime', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
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


class TestReplaceSpark(TestReplacePandas):
    config = {'engine': 'spark'}


class TestReplaceVaex(TestReplacePandas):
    config = {'engine': 'vaex'}
