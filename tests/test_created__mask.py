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


class TestMaskPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None

    def test_mask_all_all(self):
        df = self.df.copy()
        result = df.mask.all(cols='*')
        expected = self.create_dataframe(data={('NullType_Code_Multiple_attributes_date '
 'arrival_function(binary)_height(ft)_japanese name_last date seen_last '
 'position seen_rank_Cybertronian_Date '
 'Type_age_function_names_timestamp_weight(t)',
 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_all_multiple(self):
        df = self.df.copy()
        result = df.mask.all(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType_weight(t)_japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_all_numeric(self):
        df = self.df.copy()
        result = df.mask.all(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_all_string(self):
        df = self.df.copy()
        result = df.mask.all(cols=['names'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_any_all(self):
        df = self.df.copy()
        result = df.mask.any(cols='*')
        expected = self.create_dataframe(data={('NullType_Code_Multiple_attributes_date '
 'arrival_function(binary)_height(ft)_japanese name_last date seen_last '
 'position seen_rank_Cybertronian_Date '
 'Type_age_function_names_timestamp_weight(t)',
 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_any_multiple(self):
        df = self.df.copy()
        result = df.mask.any(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType_weight(t)_japanese name', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_any_numeric(self):
        df = self.df.copy()
        result = df.mask.any(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_any_string(self):
        df = self.df.copy()
        result = df.mask.any(cols=['names'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_array(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('array_test', 'object'): [[1, 2, 3, 4], [[1], [2, 3]], ['one', 'two'], ['one', ['two', [-inf]]], ['yes'], "bytearray(12, 'utf-8')"]}, force_data_types=True)
        result = df.mask.array(cols=['array_test'])
        expected = self.create_dataframe(data={('array_test', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_array_all(self):
        df = self.df.copy()
        result = df.mask.array(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_array_multiple(self):
        df = self.df.copy()
        result = df.mask.array(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_array_numeric(self):
        df = self.df.copy()
        result = df.mask.array(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_array_string(self):
        df = self.df.copy()
        result = df.mask.array(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_between_all(self):
        df = self.df.copy()
        result = df.mask.between(cols='*', lower_bound='-inf', upper_bound='inf', equal=True)
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, True, True, False, False, False], ('Multiple', 'bool'): [False, False, True, True, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [True, True, True, True, False, True], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [True, True, True, True, True, True], ('Cybertronian', 'bool'): [True, True, True, True, True, True], ('Date Type', 'bool'): [True, True, True, True, True, True], ('age', 'bool'): [True, True, True, True, True, True], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_between_multiple(self):
        df = self.df.copy()
        result = df.mask.between(cols=['age', 'NullType', 'weight(t)'], bounds=[['-inf', -10], [0, 1.9999], [300, 5000000]], equal=True)
        expected = self.create_dataframe(data={('age', 'bool'): [True, True, True, True, True, True], ('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, True, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_between_numeric(self):
        df = self.df.copy()
        result = df.mask.between(cols=['height(ft)'], bounds=[[26, -28]], equal=False)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, True, False, True, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_between_string(self):
        df = self.df.copy()
        result = df.mask.between(cols=['names'], upper_bound='-inf', lower_bound=0, equal=False)
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_boolean(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('boolean_test', 'object'): ['True', 'False', True, False, 1, 0]}, force_data_types=True)
        result = df.mask.boolean(cols=['boolean_test'])
        expected = self.create_dataframe(data={('boolean_test', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_boolean_all(self):
        df = self.df.copy()
        result = df.mask.boolean(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_boolean_multiple(self):
        df = self.df.copy()
        result = df.mask.boolean(cols=['NullType', 'weight(t)', 'Cybertronian'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_boolean_numeric(self):
        df = self.df.copy()
        result = df.mask.boolean(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_boolean_string(self):
        df = self.df.copy()
        result = df.mask.boolean(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_contains_all(self):
        df = self.df.copy()
        result = df.mask.contains(cols='*', value='a')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [True, True, True, True, True, True], ('height(ft)', 'bool'): [False, False, False, False, True, False], ('japanese name', 'bool'): [False, True, True, False, True, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, True], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [True, True, False, True, False, True], ('names', 'bool'): [False, False, False, True, True, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_contains_multiple(self):
        df = self.df.copy()
        result = df.mask.contains(cols=['NullType', 'weight(t)', 'Cybertronian'], value='T|N', case=True, flags=1, na=True, regex=True)
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_contains_numeric(self):
        df = self.df.copy()
        result = df.mask.contains(cols=['height(ft)'], value='0', flags=0, na=True, regex=False)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [True, True, True, True, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_contains_string(self):
        df = self.df.copy()
        result = df.mask.contains(cols=['function'], value='Le.', case=True, flags=3, na=False, regex=True)
        expected = self.create_dataframe(data={('function', 'bool'): [True, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_credit_card_number(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('credit_card_number_test', 'object'): ['2345 6362 6362 8632', 5692857295730750, '31028482204828450', '99 77 80 14 53 73 83 53', '8 5 0 0 1 5 8 1 5 8 3 7 0 0 0 1', 10]}, force_data_types=True)
        result = df.mask.credit_card_number(cols=['credit_card_number_test'])
        expected = self.create_dataframe(data={('credit_card_number_test', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_credit_card_number_all(self):
        df = self.df.copy()
        result = df.mask.credit_card_number(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_credit_card_number_multiple(self):
        df = self.df.copy()
        result = df.mask.credit_card_number(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_credit_card_number_numeric(self):
        df = self.df.copy()
        result = df.mask.credit_card_number(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_credit_card_number_string(self):
        df = self.df.copy()
        result = df.mask.credit_card_number(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_datetime(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('datetime_test', 'object'): ['1980/04/10', '5.0', datetime.datetime(2016, 9, 10, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), '2013/06/10', datetime.datetime(2011, 4, 10, 0, 0)]}, force_data_types=True)
        result = df.mask.datetime(cols=['datetime_test'])
        expected = self.create_dataframe(data={('datetime_test', 'bool'): [False, False, True, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_datetime_all(self):
        df = self.df.copy()
        result = df.mask.datetime(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [True, True, True, True, True, True], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [True, True, True, True, True, True], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_datetime_multiple(self):
        df = self.df.copy()
        result = df.mask.datetime(cols=['Date Type', 'last date seen', 'timestamp'])
        expected = self.create_dataframe(data={('Date Type', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [True, True, True, True, True, True], ('timestamp', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_datetime_numeric(self):
        df = self.df.copy()
        result = df.mask.datetime(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_datetime_string(self):
        df = self.df.copy()
        result = df.mask.datetime(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_all(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols='*')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_all_first(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols='*', keep='first')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_all_last(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols='*', keep='last')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_multiple(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols=['NullType', 'timestamp', 'Cybertronian'], keep='first')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [False, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_multiple_first(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols=['NullType', 'timestamp', 'Cybertronian'], keep='first')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [False, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_multiple_last(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols=['NullType', 'timestamp', 'Cybertronian'], keep='first')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [False, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_numeric(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols=['rank'])
        expected = self.create_dataframe(data={('rank', 'bool'): [False, False, True, False, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_numeric_first(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols=['rank'], keep='first')
        expected = self.create_dataframe(data={('rank', 'bool'): [False, False, True, False, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_numeric_last(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols=['rank'], keep='last')
        expected = self.create_dataframe(data={('rank', 'bool'): [True, True, False, True, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_string(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_string_first(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols=['names'], keep='first')
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_duplicated_string_last(self):
        df = self.df.copy()
        result = df.mask.duplicated(cols=['names'], keep='last')
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_email(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('email_test', 'object'): ['an@example.com', 'thisisatest@gmail.com', 'somename@hotmail.com', 'an@outlook.com', 'anexample@mail.com', 'example@yahoo.com']}, force_data_types=True)
        result = df.mask.email(cols=['email_test'])
        expected = self.create_dataframe(data={('email_test', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_email_all(self):
        df = self.df.copy()
        result = df.mask.email(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_email_multiple(self):
        df = self.df.copy()
        result = df.mask.email(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_email_numeric(self):
        df = self.df.copy()
        result = df.mask.email(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_email_string(self):
        df = self.df.copy()
        result = df.mask.email(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_empty_all(self):
        df = self.df.copy()
        result = df.mask.empty(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_empty_multiple(self):
        df = self.df.copy()
        result = df.mask.empty(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_empty_numeric(self):
        df = self.df.copy()
        result = df.mask.empty(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_empty_string(self):
        df = self.df.copy()
        result = df.mask.empty(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_ends_with_all(self):
        df = self.df.copy()
        result = df.mask.ends_with(cols='*', value=']')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [True, True, True, True, True, True], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [True, True, True, True, True, True], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_ends_with_multiple(self):
        df = self.df.copy()
        result = df.mask.ends_with(cols=['NullType', 'weight(t)', 'Cybertronian'], value='one')
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_ends_with_numeric(self):
        df = self.df.copy()
        result = df.mask.ends_with(cols=['height(ft)'], value=0)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_ends_with_string(self):
        df = self.df.copy()
        result = df.mask.ends_with(cols=['function'], value='e')
        expected = self.create_dataframe(data={('function', 'bool'): [False, True, False, False, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_equal_all(self):
        df = self.df.copy()
        result = df.mask.equal(cols='*', value=10)
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [True, False, False, False, True, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_equal_multiple(self):
        df = self.df.copy()
        result = df.mask.equal(cols=['NullType', 'weight(t)', 'Cybertronian'], value=True)
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_equal_numeric(self):
        df = self.df.copy()
        result = df.mask.equal(cols=['height(ft)'], value=300)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_equal_string(self):
        df = self.df.copy()
        result = df.mask.equal(cols=['function'], value='Leader')
        expected = self.create_dataframe(data={('function', 'bool'): [True, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_expression_all_colname(self):
        df = self.df.copy()
        result = df.mask.expression(where='last position seen')
        expected = self.create_dataframe(data={('last position seen', 'bool'): [True, True, True, True, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_expression_all_expression(self):
        df = self.df.copy()
        result = df.mask.expression(where='df["rank"]>8')
        expected = self.create_dataframe(data={('rank', 'bool'): [True, False, False, False, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_expression_multiple_colname(self):
        df = self.df.copy()
        result = df.mask.expression(where='NullType')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_expression_multiple_expression(self):
        df = self.df.copy()
        result = df.mask.expression(where='df["Cybertronian"]==False')
        expected = self.create_dataframe(data={('Cybertronian', 'bool'): [False, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_expression_numeric_colname(self):
        df = self.df.copy()
        result = df.mask.expression(where='height(ft)')
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_expression_numeric_expression(self):
        df = self.df.copy()
        result = df.mask.expression(where='df["height(ft)"]>=0')
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, True, True, True, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_expression_string_colname(self):
        df = self.df.copy()
        result = df.mask.expression(where='function')
        expected = self.create_dataframe(data={('function', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_expression_string_expression(self):
        df = self.df.copy()
        result = df.mask.expression(where='df["function"]=="Leader"')
        expected = self.create_dataframe(data={('function', 'bool'): [True, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_find_all(self):
        df = self.df.copy()
        result = df.mask.find(cols='*', value='None')
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('Code', 'bool'): [False, False, False, False, True, False], ('Multiple', 'bool'): [False, False, False, False, True, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, True, True], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, True, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_find_multiple(self):
        df = self.df.copy()
        result = df.mask.find(cols=['NullType', 'weight(t)', 'Cybertronian'], value=1)
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_find_numeric(self):
        df = self.df.copy()
        result = df.mask.find(cols=['height(ft)'], value=13)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, True, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_find_string(self):
        df = self.df.copy()
        result = df.mask.find(cols=['function'], value='Leader')
        expected = self.create_dataframe(data={('function', 'bool'): [True, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_float_all(self):
        df = self.df.copy()
        result = df.mask.float(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, True, True, False, False, False], ('Multiple', 'bool'): [False, False, True, True, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [True, True, True, True, True, True], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [True, True, True, True, True, True], ('Cybertronian', 'bool'): [True, True, True, True, True, True], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [True, True, True, True, True, True], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_float_multiple(self):
        df = self.df.copy()
        result = df.mask.float(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, True], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_float_numeric(self):
        df = self.df.copy()
        result = df.mask.float(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_float_string(self):
        df = self.df.copy()
        result = df.mask.float(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_gender(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('gender_test', 'object'): ['male', 'female', 'Male', '5.0', 'MALE', 'FEMALE']}, force_data_types=True)
        result = df.mask.gender(cols=['gender_test'])
        expected = self.create_dataframe(data={('gender_test', 'bool'): [True, True, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_gender_all(self):
        df = self.df.copy()
        result = df.mask.gender(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_gender_multiple(self):
        df = self.df.copy()
        result = df.mask.gender(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_gender_numeric(self):
        df = self.df.copy()
        result = df.mask.gender(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_gender_string(self):
        df = self.df.copy()
        result = df.mask.gender(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_greater_than_all(self):
        df = self.df.copy()
        result = df.mask.greater_than(cols='*', value=10)
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, True, True, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, True, True, True, False, True], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [True, True, True, True, True, True], ('age', 'bool'): [True, True, True, True, True, True], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_greater_than_equal_all(self):
        df = self.df.copy()
        result = df.mask.greater_than_equal(cols='*', value=10)
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, True, True, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, True, True, True, False, True], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [True, False, False, False, True, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [True, True, True, True, True, True], ('age', 'bool'): [True, True, True, True, True, True], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_greater_than_equal_multiple(self):
        df = self.df.copy()
        result = df.mask.greater_than_equal(cols=['age', 'NullType', 'weight(t)'], value='-inf')
        expected = self.create_dataframe(data={('age', 'bool'): [True, True, True, True, True, True], ('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_greater_than_equal_numeric(self):
        df = self.df.copy()
        result = df.mask.greater_than_equal(cols=['height(ft)'], value=0.31)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, True, True, True, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_greater_than_equal_string(self):
        df = self.df.copy()
        result = df.mask.greater_than_equal(cols=['names'], value=0)
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_greater_than_multiple(self):
        df = self.df.copy()
        result = df.mask.greater_than(cols=['age', 'NullType', 'weight(t)'], value='-inf')
        expected = self.create_dataframe(data={('age', 'bool'): [True, True, True, True, True, True], ('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_greater_than_numeric(self):
        df = self.df.copy()
        result = df.mask.greater_than(cols=['height(ft)'], value=-0.31)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, True, True, True, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_greater_than_string(self):
        df = self.df.copy()
        result = df.mask.greater_than(cols=['names'], value=0)
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_http_code(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('http_code_test', 'object'): ['http://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'http://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test', 'http://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5']}, force_data_types=True)
        result = df.mask.http_code(cols=['http_code_test'])
        expected = self.create_dataframe(data={('http_code_test', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_http_code_all(self):
        df = self.df.copy()
        result = df.mask.http_code(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_http_code_multiple(self):
        df = self.df.copy()
        result = df.mask.http_code(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_http_code_numeric(self):
        df = self.df.copy()
        result = df.mask.http_code(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_http_code_string(self):
        df = self.df.copy()
        result = df.mask.http_code(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_int_all(self):
        df = self.df.copy()
        result = df.mask.int(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, True, True, False, False, False], ('Multiple', 'bool'): [False, False, True, True, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [True, True, True, True, False, True], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [True, True, True, True, True, True], ('Cybertronian', 'bool'): [True, True, True, True, True, True], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [True, True, True, True, True, True], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, True, True, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_int_multiple(self):
        df = self.df.copy()
        result = df.mask.int(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, True, True, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_int_numeric(self):
        df = self.df.copy()
        result = df.mask.int(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [True, True, True, True, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_int_string(self):
        df = self.df.copy()
        result = df.mask.int(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_ip(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('ip_test', 'object'): ['192.0.2.1', '192.158.1.38', '192.168.136.52', '172.16.92.107', '10.63.215.5', '10.0.5.0']}, force_data_types=True)
        result = df.mask.ip(cols=['ip_test'])
        expected = self.create_dataframe(data={('ip_test', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_ip_all(self):
        df = self.df.copy()
        result = df.mask.ip(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_ip_multiple(self):
        df = self.df.copy()
        result = df.mask.ip(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_ip_numeric(self):
        df = self.df.copy()
        result = df.mask.ip(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_ip_string(self):
        df = self.df.copy()
        result = df.mask.ip(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_less_than_all(self):
        df = self.df.copy()
        result = df.mask.less_than(cols='*', value=10)
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, True, True, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [True, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, True, True, True, False, True], ('Cybertronian', 'bool'): [True, True, True, True, True, True], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_less_than_equal_all(self):
        df = self.df.copy()
        result = df.mask.less_than_equal(cols='*', value=10)
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, True, True, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [True, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [True, True, True, True, True, True], ('Cybertronian', 'bool'): [True, True, True, True, True, True], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_less_than_equal_multiple(self):
        df = self.df.copy()
        result = df.mask.less_than_equal(cols=['age', 'NullType', 'weight(t)'], value='inf')
        expected = self.create_dataframe(data={('age', 'bool'): [True, True, True, True, True, True], ('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_less_than_equal_numeric(self):
        df = self.df.copy()
        result = df.mask.less_than_equal(cols=['height(ft)'], value=0.31)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [True, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_less_than_equal_string(self):
        df = self.df.copy()
        result = df.mask.less_than_equal(cols=['names'], value=0)
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_less_than_multiple(self):
        df = self.df.copy()
        result = df.mask.less_than(cols=['age', 'NullType', 'weight(t)'], value='inf')
        expected = self.create_dataframe(data={('age', 'bool'): [True, True, True, True, True, True], ('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_less_than_numeric(self):
        df = self.df.copy()
        result = df.mask.less_than(cols=['height(ft)'], value=0.31)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [True, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_less_than_string(self):
        df = self.df.copy()
        result = df.mask.less_than(cols=['names'], value=0)
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_missing_all(self):
        df = self.df.copy()
        result = df.mask.missing(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('Code', 'bool'): [False, False, False, False, True, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, True, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, True, True], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_missing_multiple(self):
        df = self.df.copy()
        result = df.mask.missing(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [False, False, False, False, False, True], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_missing_numeric(self):
        df = self.df.copy()
        result = df.mask.missing(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_missing_string(self):
        df = self.df.copy()
        result = df.mask.missing(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_nan_all(self):
        df = self.df.copy()
        result = df.mask.nan(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, True, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_nan_multiple(self):
        df = self.df.copy()
        result = df.mask.nan(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, True], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_nan_numeric(self):
        df = self.df.copy()
        result = df.mask.nan(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_nan_string(self):
        df = self.df.copy()
        result = df.mask.nan(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_none_all(self):
        df = self.df.copy()
        result = df.mask.none(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('Code', 'bool'): [False, False, False, False, True, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, True, True], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_none_multiple(self):
        df = self.df.copy()
        result = df.mask.none(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_none_numeric(self):
        df = self.df.copy()
        result = df.mask.none(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_none_string(self):
        df = self.df.copy()
        result = df.mask.none(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_not_equal_all(self):
        df = self.df.copy()
        result = df.mask.not_equal(cols='*', value=10)
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('Code', 'bool'): [True, True, True, True, True, True], ('Multiple', 'bool'): [True, True, True, True, True, True], ('attributes', 'bool'): [True, True, True, True, True, True], ('date arrival', 'bool'): [True, True, True, True, True, True], ('function(binary)', 'bool'): [True, True, True, True, True, True], ('height(ft)', 'bool'): [True, True, True, True, True, True], ('japanese name', 'bool'): [True, True, True, True, True, True], ('last date seen', 'bool'): [True, True, True, True, True, True], ('last position seen', 'bool'): [True, True, True, True, True, True], ('rank', 'bool'): [False, True, True, True, False, True], ('Cybertronian', 'bool'): [True, True, True, True, True, True], ('Date Type', 'bool'): [True, True, True, True, True, True], ('age', 'bool'): [True, True, True, True, True, True], ('function', 'bool'): [True, True, True, True, True, True], ('names', 'bool'): [True, True, True, True, True, True], ('timestamp', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_not_equal_multiple(self):
        df = self.df.copy()
        result = df.mask.not_equal(cols=['NullType', 'weight(t)', 'Cybertronian'], value=True)
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [True, True, True, True, True, True], ('Cybertronian', 'bool'): [False, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_not_equal_numeric(self):
        df = self.df.copy()
        result = df.mask.not_equal(cols=['height(ft)'], value=300)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_not_equal_string(self):
        df = self.df.copy()
        result = df.mask.not_equal(cols=['function'], value='Leader')
        expected = self.create_dataframe(data={('function', 'bool'): [False, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_null_all(self):
        df = self.df.copy()
        result = df.mask.null(cols='*', how='all')
        expected = self.create_dataframe(data={('__null__', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_null_multiple(self):
        df = self.df.copy()
        result = df.mask.null(cols=['NullType', 'weight(t)', 'japanese name'], how='any')
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [False, False, False, False, False, True], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_null_numeric(self):
        df = self.df.copy()
        result = df.mask.null(cols=['height(ft)'], how='any')
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_null_string(self):
        df = self.df.copy()
        result = df.mask.null(cols=['names'], how='all')
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_numeric_all(self):
        df = self.df.copy()
        result = df.mask.numeric(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, True, True, False, False, False], ('Multiple', 'bool'): [False, False, True, True, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [True, True, True, True, True, True], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [True, True, True, True, True, True], ('Cybertronian', 'bool'): [True, True, True, True, True, True], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [True, True, True, True, True, True], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_numeric_multiple(self):
        df = self.df.copy()
        result = df.mask.numeric(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [True, True, True, True, True, True], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_numeric_numeric(self):
        df = self.df.copy()
        result = df.mask.numeric(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_numeric_string(self):
        df = self.df.copy()
        result = df.mask.numeric(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_object(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('object_test', 'object'): ['1980/04/10', True, None, inf, 'yes', "bytearray(12, 'utf-8')"]}, force_data_types=True)
        result = df.mask.object(cols=['object_test'])
        expected = self.create_dataframe(data={('object_test', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_object_all(self):
        df = self.df.copy()
        result = df.mask.object(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('Code', 'bool'): [True, True, True, True, True, True], ('Multiple', 'bool'): [True, True, True, True, True, True], ('attributes', 'bool'): [True, True, True, True, True, True], ('date arrival', 'bool'): [True, True, True, True, True, True], ('function(binary)', 'bool'): [True, True, True, True, True, True], ('height(ft)', 'bool'): [True, True, True, True, True, True], ('japanese name', 'bool'): [True, True, True, True, True, True], ('last date seen', 'bool'): [True, True, True, True, True, True], ('last position seen', 'bool'): [True, True, True, True, True, True], ('rank', 'bool'): [True, True, True, True, True, True], ('Cybertronian', 'bool'): [True, True, True, True, True, True], ('Date Type', 'bool'): [True, True, True, True, True, True], ('age', 'bool'): [True, True, True, True, True, True], ('function', 'bool'): [True, True, True, True, True, True], ('names', 'bool'): [True, True, True, True, True, True], ('timestamp', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_object_multiple(self):
        df = self.df.copy()
        result = df.mask.object(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [True, True, True, True, True, True], ('japanese name', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_object_numeric(self):
        df = self.df.copy()
        result = df.mask.object(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_object_string(self):
        df = self.df.copy()
        result = df.mask.object(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_pattern_all(self):
        df = self.df.copy()
        result = df.mask.pattern(cols='*', pattern='**cc**')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_pattern_multiple(self):
        df = self.df.copy()
        result = df.mask.pattern(cols=['NullType', 'weight(t)', 'Cybertronian'], pattern='****')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_pattern_numeric(self):
        df = self.df.copy()
        result = df.mask.pattern(cols=['height(ft)'], pattern='##!#')
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_pattern_string(self):
        df = self.df.copy()
        result = df.mask.pattern(cols=['function'], pattern='Ullclc')
        expected = self.create_dataframe(data={('function', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_phone_number(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('phone_number_test', 'object'): [5516528967, '55 8395 1284', '+52 55 3913 1941', '+1 (210) 589-6721', 12106920692, '5532592785']}, force_data_types=True)
        result = df.mask.phone_number(cols=['phone_number_test'])
        expected = self.create_dataframe(data={('phone_number_test', 'bool'): [True, False, False, False, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_phone_number_all(self):
        df = self.df.copy()
        result = df.mask.phone_number(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_phone_number_multiple(self):
        df = self.df.copy()
        result = df.mask.phone_number(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_phone_number_numeric(self):
        df = self.df.copy()
        result = df.mask.phone_number(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_phone_number_string(self):
        df = self.df.copy()
        result = df.mask.phone_number(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_social_security_number(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('social_security_number_test', 'object'): [372847278, '551-83-1284', '525 93 1941', '230 89-6721', '121-069 2062', '371847288']}, force_data_types=True)
        result = df.mask.social_security_number(cols=['social_security_number_test'])
        expected = self.create_dataframe(data={('social_security_number_test', 'bool'): [False, True, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_social_security_number_all(self):
        df = self.df.copy()
        result = df.mask.social_security_number(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_social_security_number_multiple(self):
        df = self.df.copy()
        result = df.mask.social_security_number(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_social_security_number_numeric(self):
        df = self.df.copy()
        result = df.mask.social_security_number(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_social_security_number_string(self):
        df = self.df.copy()
        result = df.mask.social_security_number(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_starts_with_all(self):
        df = self.df.copy()
        result = df.mask.starts_with(cols='*', value='N')
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('Code', 'bool'): [False, False, False, False, True, False], ('Multiple', 'bool'): [False, False, False, False, True, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, True, True], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, True, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_starts_with_multiple(self):
        df = self.df.copy()
        result = df.mask.starts_with(cols=['NullType', 'weight(t)', 'Cybertronian'], value=True)
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_starts_with_numeric(self):
        df = self.df.copy()
        result = df.mask.starts_with(cols=['height(ft)'], value=1)
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_starts_with_string(self):
        df = self.df.copy()
        result = df.mask.starts_with(cols=['function'], value='Lead')
        expected = self.create_dataframe(data={('function', 'bool'): [True, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_str_all(self):
        df = self.df.copy()
        result = df.mask.str(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [True, True, False, True, False, True], ('Multiple', 'bool'): [True, True, False, True, True, True], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [True, True, True, True, True, True], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [True, True, True, True, True, True], ('last position seen', 'bool'): [True, True, True, True, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [True, True, True, True, True, True], ('names', 'bool'): [True, True, True, True, True, True], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_str_multiple(self):
        df = self.df.copy()
        result = df.mask.str(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_str_numeric(self):
        df = self.df.copy()
        result = df.mask.str(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_str_string(self):
        df = self.df.copy()
        result = df.mask.str(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_all(self):
        df = self.df.copy()
        result = df.mask.unique(cols='*')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_all_first(self):
        df = self.df.copy()
        result = df.mask.unique(cols='*', keep='first')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_all_last(self):
        df = self.df.copy()
        result = df.mask.unique(cols='*', keep='last')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_multiple(self):
        df = self.df.copy()
        result = df.mask.unique(cols=['NullType', 'timestamp', 'Cybertronian'], keep='first')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [True, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_multiple_first(self):
        df = self.df.copy()
        result = df.mask.unique(cols=['NullType', 'timestamp', 'Cybertronian'], keep='first')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [True, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_multiple_last(self):
        df = self.df.copy()
        result = df.mask.unique(cols=['NullType', 'timestamp', 'Cybertronian'], keep='first')
        expected = self.create_dataframe(data={('__duplicated__', 'bool'): [True, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_numeric(self):
        df = self.df.copy()
        result = df.mask.unique(cols=['rank'])
        expected = self.create_dataframe(data={('rank', 'bool'): [True, True, False, True, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_numeric_first(self):
        df = self.df.copy()
        result = df.mask.unique(cols=['rank'], keep='first')
        expected = self.create_dataframe(data={('rank', 'bool'): [True, True, False, True, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_numeric_last(self):
        df = self.df.copy()
        result = df.mask.unique(cols=['rank'], keep='last')
        expected = self.create_dataframe(data={('rank', 'bool'): [False, False, True, False, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_string(self):
        df = self.df.copy()
        result = df.mask.unique(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_string_first(self):
        df = self.df.copy()
        result = df.mask.unique(cols=['names'], keep='first')
        expected = self.create_dataframe(data={('names', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_unique_string_last(self):
        df = self.df.copy()
        result = df.mask.unique(cols=['names'], keep='last')
        expected = self.create_dataframe(data={('names', 'bool'): [True, True, True, True, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_url(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('urls_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test', 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5']}, force_data_types=True)
        result = df.mask.url(cols=['urls_test'])
        expected = self.create_dataframe(data={('urls_test', 'bool'): [True, False, True, False, True, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_url_all(self):
        df = self.df.copy()
        result = df.mask.url(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_url_multiple(self):
        df = self.df.copy()
        result = df.mask.url(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_url_numeric(self):
        df = self.df.copy()
        result = df.mask.url(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_url_string(self):
        df = self.df.copy()
        result = df.mask.url(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_value_in_all(self):
        df = self.df.copy()
        result = df.mask.value_in(cols='*', values=[10, True, None, 'Jazz'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('Code', 'bool'): [False, False, False, False, True, False], ('Multiple', 'bool'): [False, False, True, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, True, True], ('rank', 'bool'): [True, False, False, False, True, False], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, True, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_value_in_multiple(self):
        df = self.df.copy()
        result = df.mask.value_in(cols=['NullType', 'weight(t)', 'Cybertronian'], values=[False, None, 4])
        expected = self.create_dataframe(data={('NullType', 'bool'): [True, True, True, True, True, True], ('weight(t)', 'bool'): [False, False, True, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_value_in_numeric(self):
        df = self.df.copy()
        result = df.mask.value_in(cols=['height(ft)'], values=[300, 'nan'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, True]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_value_in_string(self):
        df = self.df.copy()
        result = df.mask.value_in(cols=['function'], values='Leader')
        expected = self.create_dataframe(data={('function', 'bool'): [True, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_zip_code(self):
        df = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('Code', 'object'): ['123A', '456', 456, 'e', None, '{code}'], ('Multiple', 'object'): ['12/12/12', 'True', 1, '0.0', 'None', '{}'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('zip_code_test', 'int64'): [90210, 21252, 36104, 99801, 85001, 10]}, force_data_types=True)
        result = df.mask.zip_code(cols=['zip_code_test'])
        expected = self.create_dataframe(data={('zip_code_test', 'bool'): [True, True, True, True, True, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_zip_code_all(self):
        df = self.df.copy()
        result = df.mask.zip_code(cols='*')
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('Code', 'bool'): [False, False, False, False, False, False], ('Multiple', 'bool'): [False, False, False, False, False, False], ('attributes', 'bool'): [False, False, False, False, False, False], ('date arrival', 'bool'): [False, False, False, False, False, False], ('function(binary)', 'bool'): [False, False, False, False, False, False], ('height(ft)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False], ('last date seen', 'bool'): [False, False, False, False, False, False], ('last position seen', 'bool'): [False, False, False, False, False, False], ('rank', 'bool'): [False, False, False, False, False, False], ('Cybertronian', 'bool'): [False, False, False, False, False, False], ('Date Type', 'bool'): [False, False, False, False, False, False], ('age', 'bool'): [False, False, False, False, False, False], ('function', 'bool'): [False, False, False, False, False, False], ('names', 'bool'): [False, False, False, False, False, False], ('timestamp', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_zip_code_multiple(self):
        df = self.df.copy()
        result = df.mask.zip_code(cols=['NullType', 'weight(t)', 'japanese name'])
        expected = self.create_dataframe(data={('NullType', 'bool'): [False, False, False, False, False, False], ('weight(t)', 'bool'): [False, False, False, False, False, False], ('japanese name', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_zip_code_numeric(self):
        df = self.df.copy()
        result = df.mask.zip_code(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_mask_zip_code_string(self):
        df = self.df.copy()
        result = df.mask.zip_code(cols=['names'])
        expected = self.create_dataframe(data={('names', 'bool'): [False, False, False, False, False, False]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))


class TestMaskDask(TestMaskPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestMaskPartitionDask(TestMaskPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMaskCUDF(TestMaskPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMaskDC(TestMaskPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMaskPartitionDC(TestMaskPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMaskSpark(TestMaskPandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMaskVaex(TestMaskPandas):
        config = {'engine': 'vaex'}
