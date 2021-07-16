from optimus.tests.base import TestBase
import datetime
Timestamp = lambda t: datetime.datetime.strptime(t,"%Y-%m-%d %H:%M:%S")
nan = float("nan")
inf = float("inf")
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort

class TestColsPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int32'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'string'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'string'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None
    
    def test_cols_abs_all(self):
        df = self.df
        result = df.cols.abs(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_abs_bool(self):
        df = self.df
        result = df.cols.abs(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.0, 1.0, 1.0, 1.0, 1.0, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_abs_bytearray(self):
        df = self.df
        result = df.cols.abs(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_abs_datetime(self):
        df = self.df
        result = df.cols.abs(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_abs_list(self):
        df = self.df
        result = df.cols.abs(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_abs_multiple(self):
        df = self.df
        result = df.cols.abs(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_abs_NoneType(self):
        df = self.df
        result = df.cols.abs(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_abs_numeric_float(self):
        df = self.df
        result = df.cols.abs(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_abs_numeric_int(self):
        df = self.df
        result = df.cols.abs(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10.0, 7.0, 7.0, 8.0, 10.0, 8.0],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_abs_string(self):
        df = self.df
        result = df.cols.abs(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acosh_all(self):
        df = self.df
        result = df.cols.acosh(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acosh_bool(self):
        df = self.df
        result = df.cols.acosh(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.0, 0.0, 0.0, 0.0, 0.0, nan],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acosh_bytearray(self):
        df = self.df
        result = df.cols.acosh(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acosh_datetime(self):
        df = self.df
        result = df.cols.acosh(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acosh_list(self):
        df = self.df
        result = df.cols.acosh(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acosh_multiple(self):
        df = self.df
        result = df.cols.acosh(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_acosh_NoneType(self):
        df = self.df
        result = df.cols.acosh(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acosh_numeric_float(self):
        df = self.df
        result = df.cols.acosh(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [nan, 3.5254943480781717, 3.95087369077445, 3.2566139548000526, nan, 6.396926877426794],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acosh_numeric_int(self):
        df = self.df
        result = df.cols.acosh(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [2.993222846126381, 2.6339157938496336, 2.6339157938496336, 2.7686593833135738, 2.993222846126381, 2.7686593833135738],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acosh_string(self):
        df = self.df
        result = df.cols.acosh(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acos_all(self):
        df = self.df
        result = df.cols.acos(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acos_bool(self):
        df = self.df
        result = df.cols.acos(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.0, 0.0, 0.0, 0.0, 0.0, 1.5707963267948966],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acos_bytearray(self):
        df = self.df
        result = df.cols.acos(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acos_datetime(self):
        df = self.df
        result = df.cols.acos(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acos_list(self):
        df = self.df
        result = df.cols.acos(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acos_multiple(self):
        df = self.df
        result = df.cols.acos(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_acos_NoneType(self):
        df = self.df
        result = df.cols.acos(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acos_numeric_float(self):
        df = self.df
        result = df.cols.acos(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [nan, nan, nan, nan, nan, nan],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acos_numeric_int(self):
        df = self.df
        result = df.cols.acos(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [nan, nan, nan, nan, nan, nan],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_acos_string(self):
        df = self.df
        result = df.cols.acos(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asinh_all(self):
        df = self.df
        result = df.cols.asinh(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asinh_bool(self):
        df = self.df
        result = df.cols.asinh(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.881373587019543, 0.881373587019543, 0.881373587019543, 0.881373587019543, 0.881373587019543, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asinh_bytearray(self):
        df = self.df
        result = df.cols.asinh(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asinh_datetime(self):
        df = self.df
        result = df.cols.asinh(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asinh_list(self):
        df = self.df
        result = df.cols.asinh(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asinh_multiple(self):
        df = self.df
        result = df.cols.asinh(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_asinh_NoneType(self):
        df = self.df
        result = df.cols.asinh(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asinh_numeric_float(self):
        df = self.df
        result = df.cols.asinh(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-4.025670415869822, 3.5272244561999657, 3.9516133360820653, 3.2595725562629214, nan, 6.39693243298235],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asinh_numeric_int(self):
        df = self.df
        result = df.cols.asinh(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [2.99822295029797, 2.644120761058629, 2.644120761058629, 2.7764722807237177, 2.99822295029797, 2.7764722807237177],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asinh_string(self):
        df = self.df
        result = df.cols.asinh(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asin_all(self):
        df = self.df
        result = df.cols.asin(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asin_bool(self):
        df = self.df
        result = df.cols.asin(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asin_bytearray(self):
        df = self.df
        result = df.cols.asin(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asin_datetime(self):
        df = self.df
        result = df.cols.asin(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asin_list(self):
        df = self.df
        result = df.cols.asin(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asin_multiple(self):
        df = self.df
        result = df.cols.asin(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_asin_NoneType(self):
        df = self.df
        result = df.cols.asin(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asin_numeric_float(self):
        df = self.df
        result = df.cols.asin(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [nan, nan, nan, nan, nan, nan],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asin_numeric_int(self):
        df = self.df
        result = df.cols.asin(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [nan, nan, nan, nan, nan, nan],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_asin_string(self):
        df = self.df
        result = df.cols.asin(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atanh_all(self):
        df = self.df
        result = df.cols.atanh(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atanh_bool(self):
        df = self.df
        result = df.cols.atanh(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [inf, inf, inf, inf, inf, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atanh_bytearray(self):
        df = self.df
        result = df.cols.atanh(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atanh_datetime(self):
        df = self.df
        result = df.cols.atanh(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atanh_list(self):
        df = self.df
        result = df.cols.atanh(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atanh_multiple(self):
        df = self.df
        result = df.cols.atanh(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_atanh_NoneType(self):
        df = self.df
        result = df.cols.atanh(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atanh_numeric_float(self):
        df = self.df
        result = df.cols.atanh(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [nan, nan, nan, nan, nan, nan],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atanh_numeric_int(self):
        df = self.df
        result = df.cols.atanh(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [nan, nan, nan, nan, nan, nan],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atanh_string(self):
        df = self.df
        result = df.cols.atanh(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atan_all(self):
        df = self.df
        result = df.cols.atan(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atan_bool(self):
        df = self.df
        result = df.cols.atan(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.7853981633974483, 0.7853981633974483, 0.7853981633974483, 0.7853981633974483, 0.7853981633974483, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atan_bytearray(self):
        df = self.df
        result = df.cols.atan(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atan_datetime(self):
        df = self.df
        result = df.cols.atan(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atan_list(self):
        df = self.df
        result = df.cols.atan(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atan_multiple(self):
        df = self.df
        result = df.cols.atan(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_atan_NoneType(self):
        df = self.df
        result = df.cols.atan(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atan_numeric_float(self):
        df = self.df
        result = df.cols.atan(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-1.5350972141155728, 1.512040504079174, 1.5323537367737086, 1.4940244355251187, nan, 1.56746300580716],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atan_numeric_int(self):
        df = self.df
        result = df.cols.atan(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [1.4711276743037347, 1.4288992721907328, 1.4288992721907328, 1.446441332248135, 1.4711276743037347, 1.446441332248135],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_atan_string(self):
        df = self.df
        result = df.cols.atan(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_capitalize_all(self):
        df = self.df
        result = df.cols.capitalize(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[none, 5700.0]', '[91.44, none]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["Bytearray(b'leader')", "Bytearray(b'espionage')", "Bytearray(b'security')", "Bytearray(b'first lieutenant')", "Bytearray(b'none')", "Bytearray(b'battle station')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0'],
 'japanese name': ["['inochi', 'convoy']", "['bumble', 'goldback']", "['roadbuster']", "['meister']", "['megatron']", "['metroflex']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First lieutenant', 'None', 'Battle station'],
 'names': ['Optimus', 'Bumbl#ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_capitalize_bool(self):
        df = self.df
        result = df.cols.capitalize(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_capitalize_bytearray(self):
        df = self.df
        result = df.cols.capitalize(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["Bytearray(b'leader')", "Bytearray(b'espionage')", "Bytearray(b'security')", "Bytearray(b'first lieutenant')", "Bytearray(b'none')", "Bytearray(b'battle station')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_capitalize_datetime(self):
        df = self.df
        result = df.cols.capitalize(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_capitalize_list(self):
        df = self.df
        result = df.cols.capitalize(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[none, 5700.0]', '[91.44, none]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_capitalize_multiple(self):
        df = self.df
        result = df.cols.capitalize(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['inochi', 'convoy']", "['bumble', 'goldback']", "['roadbuster']", "['meister']", "['megatron']", "['metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First lieutenant', 'None', 'Battle station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_capitalize_NoneType(self):
        df = self.df
        result = df.cols.capitalize(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_capitalize_numeric_float(self):
        df = self.df
        result = df.cols.capitalize(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_capitalize_numeric_int(self):
        df = self.df
        result = df.cols.capitalize(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_capitalize_string(self):
        df = self.df
        result = df.cols.capitalize(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'Bumbl#ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ceil_all(self):
        df = self.df
        result = df.cols.ceil(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ceil_bool(self):
        df = self.df
        result = df.cols.ceil(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.0, 1.0, 1.0, 1.0, 1.0, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ceil_bytearray(self):
        df = self.df
        result = df.cols.ceil(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ceil_datetime(self):
        df = self.df
        result = df.cols.ceil(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ceil_list(self):
        df = self.df
        result = df.cols.ceil(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ceil_multiple(self):
        df = self.df
        result = df.cols.ceil(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_ceil_NoneType(self):
        df = self.df
        result = df.cols.ceil(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ceil_numeric_float(self):
        df = self.df
        result = df.cols.ceil(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ceil_numeric_int(self):
        df = self.df
        result = df.cols.ceil(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10.0, 7.0, 7.0, 8.0, 10.0, 8.0],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ceil_string(self):
        df = self.df
        result = df.cols.ceil(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_copy_all(self):
        df = self.df
        result = df.cols.copy(cols='*')
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_copy_bool(self):
        df = self.df
        result = df.cols.copy(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_copy_bytearray(self):
        df = self.df
        result = df.cols.copy(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_copy_datetime(self):
        df = self.df
        result = df.cols.copy(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_copy_list(self):
        df = self.df
        result = df.cols.copy(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_copy_multiple(self):
        df = self.df
        result = df.cols.copy(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_copy_NoneType(self):
        df = self.df
        result = df.cols.copy(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_copy_numeric_float(self):
        df = self.df
        result = df.cols.copy(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_copy_numeric_int(self):
        df = self.df
        result = df.cols.copy(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_copy_string(self):
        df = self.df
        result = df.cols.copy(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cosh_all(self):
        df = self.df
        result = df.cols.cosh(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cosh_bool(self):
        df = self.df
        result = df.cols.cosh(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.5430806348152437, 1.5430806348152437, 1.5430806348152437, 1.5430806348152437, 1.5430806348152437, 1.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cosh_bytearray(self):
        df = self.df
        result = df.cols.cosh(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cosh_datetime(self):
        df = self.df
        result = df.cols.cosh(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cosh_list(self):
        df = self.df
        result = df.cols.cosh(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cosh_multiple(self):
        df = self.df
        result = df.cols.cosh(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_cosh_NoneType(self):
        df = self.df
        result = df.cols.cosh(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cosh_numeric_float(self):
        df = self.df
        result = df.cols.cosh(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [723128532145.7375, 12077476.37678767, 97864804714.41939, 221206.6960055904, nan, 9.712131976206279e+129],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cosh_numeric_int(self):
        df = self.df
        result = df.cols.cosh(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [11013.232920103323, 548.3170351552121, 548.3170351552121, 1490.479161252178, 11013.232920103323, 1490.479161252178],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cosh_string(self):
        df = self.df
        result = df.cols.cosh(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cos_all(self):
        df = self.df
        result = df.cols.cos(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cos_bool(self):
        df = self.df
        result = df.cols.cos(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.5403023058681398, 0.5403023058681398, 0.5403023058681398, 0.5403023058681398, 0.5403023058681398, 1.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cos_bytearray(self):
        df = self.df
        result = df.cols.cos(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cos_datetime(self):
        df = self.df
        result = df.cols.cos(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cos_list(self):
        df = self.df
        result = df.cols.cos(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cos_multiple(self):
        df = self.df
        result = df.cols.cos(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_cos_NoneType(self):
        df = self.df
        result = df.cols.cos(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cos_numeric_float(self):
        df = self.df
        result = df.cols.cos(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-0.9626058663135666, -0.27516333805159693, 0.6469193223286404, 0.9074467814501962, nan, -0.022096619278683942],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cos_numeric_int(self):
        df = self.df
        result = df.cols.cos(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [-0.8390715290764524, 0.7539022543433046, 0.7539022543433046, -0.14550003380861354, -0.8390715290764524, -0.14550003380861354],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_cos_string(self):
        df = self.df
        result = df.cols.cos(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_domain_all(self):
        df = self.df
        result = df.cols.domain(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_domain_bool(self):
        df = self.df
        result = df.cols.domain(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_domain_bytearray(self):
        df = self.df
        result = df.cols.domain(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_domain_datetime(self):
        df = self.df
        result = df.cols.domain(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_domain_list(self):
        df = self.df
        result = df.cols.domain(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_domain_multiple(self):
        df = self.df
        result = df.cols.domain(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_domain_NoneType(self):
        df = self.df
        result = df.cols.domain(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_domain_numeric_float(self):
        df = self.df
        result = df.cols.domain(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_domain_numeric_int(self):
        df = self.df
        result = df.cols.domain(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_domain_string(self):
        df = self.df
        result = df.cols.domain(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [None, None, None, None, None, None],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_metaphone_all(self):
        df = self.df
        result = df.cols.double_metaphone(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_metaphone_bool(self):
        df = self.df
        result = df.cols.double_metaphone(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_metaphone_bytearray(self):
        df = self.df
        result = df.cols.double_metaphone(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_metaphone_datetime(self):
        df = self.df
        result = df.cols.double_metaphone(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_metaphone_list(self):
        df = self.df
        result = df.cols.double_metaphone(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_metaphone_multiple(self):
        df = self.df
        result = df.cols.double_metaphone(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_double_metaphone_NoneType(self):
        df = self.df
        result = df.cols.double_metaphone(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_metaphone_numeric_float(self):
        df = self.df
        result = df.cols.double_metaphone(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_metaphone_numeric_int(self):
        df = self.df
        result = df.cols.double_metaphone(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_metaphone_string(self):
        df = self.df
        result = df.cols.double_metaphone(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [('APTMS', ''), ('PMPLLP', ''), ('ARNT', ''), ('JS', 'AS'), ('MKTRN', ''), ('MTRPLKSKSKSKSKS', '')],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_methaphone_all(self):
        df = self.df
        result = df.cols.double_methaphone(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_methaphone_bool(self):
        df = self.df
        result = df.cols.double_methaphone(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_methaphone_bytearray(self):
        df = self.df
        result = df.cols.double_methaphone(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_methaphone_datetime(self):
        df = self.df
        result = df.cols.double_methaphone(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_methaphone_list(self):
        df = self.df
        result = df.cols.double_methaphone(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_methaphone_multiple(self):
        df = self.df
        result = df.cols.double_methaphone(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_double_methaphone_NoneType(self):
        df = self.df
        result = df.cols.double_methaphone(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_methaphone_numeric_float(self):
        df = self.df
        result = df.cols.double_methaphone(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_methaphone_numeric_int(self):
        df = self.df
        result = df.cols.double_methaphone(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_double_methaphone_string(self):
        df = self.df
        result = df.cols.double_methaphone(cols=['names'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_domain_all(self):
        df = self.df
        result = df.cols.email_domain(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_domain_bool(self):
        df = self.df
        result = df.cols.email_domain(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_domain_bytearray(self):
        df = self.df
        result = df.cols.email_domain(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_domain_datetime(self):
        df = self.df
        result = df.cols.email_domain(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_domain_list(self):
        df = self.df
        result = df.cols.email_domain(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_domain_multiple(self):
        df = self.df
        result = df.cols.email_domain(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_email_domain_NoneType(self):
        df = self.df
        result = df.cols.email_domain(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_domain_numeric_float(self):
        df = self.df
        result = df.cols.email_domain(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_domain_numeric_int(self):
        df = self.df
        result = df.cols.email_domain(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_domain_string(self):
        df = self.df
        result = df.cols.email_domain(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_username_all(self):
        df = self.df
        result = df.cols.email_username(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_username_bool(self):
        df = self.df
        result = df.cols.email_username(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_username_bytearray(self):
        df = self.df
        result = df.cols.email_username(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_username_datetime(self):
        df = self.df
        result = df.cols.email_username(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_username_list(self):
        df = self.df
        result = df.cols.email_username(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_username_multiple(self):
        df = self.df
        result = df.cols.email_username(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_email_username_NoneType(self):
        df = self.df
        result = df.cols.email_username(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_username_numeric_float(self):
        df = self.df
        result = df.cols.email_username(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_username_numeric_int(self):
        df = self.df
        result = df.cols.email_username(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_email_username_string(self):
        df = self.df
        result = df.cols.email_username(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_expand_contrated_words_all(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_expand_contrated_words_bool(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_expand_contrated_words_bytearray(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_expand_contrated_words_datetime(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_expand_contrated_words_list(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_expand_contrated_words_multiple(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_expand_contrated_words_NoneType(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_expand_contrated_words_numeric_float(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_expand_contrated_words_numeric_int(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_expand_contrated_words_string(self):
        df = self.df
        result = df.cols.expand_contrated_words(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_exp_all(self):
        df = self.df
        result = df.cols.exp(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_exp_bool(self):
        df = self.df
        result = df.cols.exp(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [2.718281828459045, 2.718281828459045, 2.718281828459045, 2.718281828459045, 2.718281828459045, 1.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_exp_bytearray(self):
        df = self.df
        result = df.cols.exp(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_exp_datetime(self):
        df = self.df
        result = df.cols.exp(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_exp_list(self):
        df = self.df
        result = df.cols.exp(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_exp_multiple(self):
        df = self.df
        result = df.cols.exp(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_exp_NoneType(self):
        df = self.df
        result = df.cols.exp(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_exp_numeric_float(self):
        df = self.df
        result = df.cols.exp(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [6.914400106940203e-13, 24154952.7535753, 195729609428.83878, 442413.3920089205, nan, 1.9424263952412558e+130],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_exp_numeric_int(self):
        df = self.df
        result = df.cols.exp(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [22026.465794806718, 1096.6331584284585, 1096.6331584284585, 2980.9579870417283, 22026.465794806718, 2980.9579870417283],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_exp_string(self):
        df = self.df
        result = df.cols.exp(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_fingerprint_all(self):
        df = self.df
        result = df.cols.fingerprint(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['none', 'none', 'none', 'none', 'none', 'none'],
 'attributes': ['43000 85344', '20000 5334', '40000 79248', '18000 39624', '57000 none', '9144 none'],
 'date arrival': ['19800410', '19800410', '19800410', '19800410', '19800410', '19800410'],
 'function(binary)': ['bytearraybleader', 'bytearraybespionage', 'bytearraybsecurity', 'bytearraybfirst lieutenant', 'bytearraybnone', 'bytearraybbattle station'],
 'height(ft)': ['280', '170', '260', '130', 'nan', '3000'],
 'japanese name': ['convoy inochi', 'bumble goldback', 'roadbuster', 'meister', 'megatron', 'metroflex'],
 'last date seen': ['20160910', '20150810', '20140710', '20130610', '20120510', '20110410'],
 'last position seen': ['1944273599201111', '1064270771612534', '37789563122400356', '33670666117841553', 'none', 'none'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['true', 'true', 'true', 'true', 'true', 'false'],
 'Date Type': ['20160910', '20150810', '20140624', '20130624', '20120510', '20110410'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['leader', 'espionage', 'security', 'first lieutenant', 'none', 'battle station'],
 'names': ['optimus', 'bumblebee', 'ironhide', 'jazz', 'megatron', 'metroplex'],
 'timestamp': ['20140624', '20140624', '20140624', '20140624', '20140624', '20140624'],
 'weight(t)': ['43', '20', '40', '18', '57', 'nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_fingerprint_bool(self):
        df = self.df
        result = df.cols.fingerprint(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['true', 'true', 'true', 'true', 'true', 'false'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_fingerprint_bytearray(self):
        df = self.df
        result = df.cols.fingerprint(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ['bytearraybleader', 'bytearraybespionage', 'bytearraybsecurity', 'bytearraybfirst lieutenant', 'bytearraybnone', 'bytearraybbattle station'],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_fingerprint_datetime(self):
        df = self.df
        result = df.cols.fingerprint(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['20160910', '20150810', '20140710', '20130610', '20120510', '20110410'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_fingerprint_list(self):
        df = self.df
        result = df.cols.fingerprint(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['43000 85344', '20000 5334', '40000 79248', '18000 39624', '57000 none', '9144 none'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_fingerprint_multiple(self):
        df = self.df
        result = df.cols.fingerprint(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['none', 'none', 'none', 'none', 'none', 'none'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['19800410', '19800410', '19800410', '19800410', '19800410', '19800410'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ['convoy inochi', 'bumble goldback', 'roadbuster', 'meister', 'megatron', 'metroflex'],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['1944273599201111', '1064270771612534', '37789563122400356', '33670666117841553', 'none', 'none'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['20160910', '20150810', '20140624', '20130624', '20120510', '20110410'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['leader', 'espionage', 'security', 'first lieutenant', 'none', 'battle station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['20140624', '20140624', '20140624', '20140624', '20140624', '20140624'],
 'weight(t)': ['43', '20', '40', '18', '57', 'nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_fingerprint_NoneType(self):
        df = self.df
        result = df.cols.fingerprint(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['none', 'none', 'none', 'none', 'none', 'none'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_fingerprint_numeric_float(self):
        df = self.df
        result = df.cols.fingerprint(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['280', '170', '260', '130', 'nan', '3000'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_fingerprint_numeric_int(self):
        df = self.df
        result = df.cols.fingerprint(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_fingerprint_string(self):
        df = self.df
        result = df.cols.fingerprint(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['optimus', 'bumblebee', 'ironhide', 'jazz', 'megatron', 'metroplex'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_floor_all(self):
        df = self.df
        result = df.cols.floor(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_floor_bool(self):
        df = self.df
        result = df.cols.floor(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.0, 1.0, 1.0, 1.0, 1.0, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_floor_bytearray(self):
        df = self.df
        result = df.cols.floor(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_floor_datetime(self):
        df = self.df
        result = df.cols.floor(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_floor_list(self):
        df = self.df
        result = df.cols.floor(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_floor_multiple(self):
        df = self.df
        result = df.cols.floor(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_floor_NoneType(self):
        df = self.df
        result = df.cols.floor(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_floor_numeric_float(self):
        df = self.df
        result = df.cols.floor(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_floor_numeric_int(self):
        df = self.df
        result = df.cols.floor(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10.0, 7.0, 7.0, 8.0, 10.0, 8.0],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_floor_string(self):
        df = self.df
        result = df.cols.floor(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_host_all(self):
        df = self.df
        result = df.cols.host(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_host_bool(self):
        df = self.df
        result = df.cols.host(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_host_bytearray(self):
        df = self.df
        result = df.cols.host(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_host_datetime(self):
        df = self.df
        result = df.cols.host(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_host_list(self):
        df = self.df
        result = df.cols.host(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_host_multiple(self):
        df = self.df
        result = df.cols.host(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_host_NoneType(self):
        df = self.df
        result = df.cols.host(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_host_numeric_float(self):
        df = self.df
        result = df.cols.host(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_host_numeric_int(self):
        df = self.df
        result = df.cols.host(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_host_string(self):
        df = self.df
        result = df.cols.host(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl', 'ironhide', 'Jazz', 'Megatron', 'Metroplex'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_index_to_string_all(self):
        df = self.df
        result = df.cols.index_to_string(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_index_to_string_bool(self):
        df = self.df
        result = df.cols.index_to_string(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_index_to_string_bytearray(self):
        df = self.df
        result = df.cols.index_to_string(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_index_to_string_datetime(self):
        df = self.df
        result = df.cols.index_to_string(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_index_to_string_list(self):
        df = self.df
        result = df.cols.index_to_string(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_index_to_string_multiple(self):
        df = self.df
        result = df.cols.index_to_string(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_index_to_string_NoneType(self):
        df = self.df
        result = df.cols.index_to_string(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_index_to_string_numeric_float(self):
        df = self.df
        result = df.cols.index_to_string(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_index_to_string_numeric_int(self):
        df = self.df
        result = df.cols.index_to_string(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_index_to_string_string(self):
        df = self.df
        result = df.cols.index_to_string(cols=['names'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_infer_dtypes_all(self):
        df = self.df
        result = df.cols.infer_dtypes(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['null', 'null', 'null', 'null', 'null', 'null'],
 'attributes': ['array', 'array', 'array', 'array', 'array', 'array'],
 'date arrival': ['str', 'str', 'str', 'str', 'str', 'str'],
 'function(binary)': ['object', 'object', 'object', 'object', 'object', 'object'],
 'height(ft)': ['int', 'int', 'int', 'int', 'null', 'int'],
 'japanese name': ['array', 'array', 'array', 'array', 'array', 'array'],
 'last date seen': ['datetime', 'datetime', 'datetime', 'datetime', 'datetime', 'datetime'],
 'last position seen': ['str', 'str', 'str', 'str', 'null', 'null'],
 'rank': ['int', 'int', 'int', 'int', 'int', 'int'],
 'Cybertronian': ['boolean', 'boolean', 'boolean', 'boolean', 'boolean', 'boolean'],
 'Date Type': ['datetime', 'datetime', 'datetime', 'datetime', 'datetime', 'datetime'],
 'age': ['int', 'int', 'int', 'int', 'int', 'int'],
 'function': ['str', 'str', 'str', 'str', 'str', 'str'],
 'names': ['str', 'str', 'str', 'str', 'str', 'str'],
 'timestamp': ['datetime', 'datetime', 'datetime', 'datetime', 'datetime', 'datetime'],
 'weight(t)': ['decimal', 'int', 'int', 'decimal', 'decimal', 'null']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_infer_dtypes_bool(self):
        df = self.df
        result = df.cols.infer_dtypes(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['boolean', 'boolean', 'boolean', 'boolean', 'boolean', 'boolean'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_infer_dtypes_bytearray(self):
        df = self.df
        result = df.cols.infer_dtypes(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ['object', 'object', 'object', 'object', 'object', 'object'],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_infer_dtypes_datetime(self):
        df = self.df
        result = df.cols.infer_dtypes(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['datetime', 'datetime', 'datetime', 'datetime', 'datetime', 'datetime'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_infer_dtypes_list(self):
        df = self.df
        result = df.cols.infer_dtypes(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['array', 'array', 'array', 'array', 'array', 'array'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_infer_dtypes_multiple(self):
        df = self.df
        result = df.cols.infer_dtypes(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['null', 'null', 'null', 'null', 'null', 'null'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['str', 'str', 'str', 'str', 'str', 'str'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ['array', 'array', 'array', 'array', 'array', 'array'],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['str', 'str', 'str', 'str', 'null', 'null'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['datetime', 'datetime', 'datetime', 'datetime', 'datetime', 'datetime'],
 'age': ['int', 'int', 'int', 'int', 'int', 'int'],
 'function': ['str', 'str', 'str', 'str', 'str', 'str'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['datetime', 'datetime', 'datetime', 'datetime', 'datetime', 'datetime'],
 'weight(t)': ['decimal', 'int', 'int', 'decimal', 'decimal', 'null']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_infer_dtypes_NoneType(self):
        df = self.df
        result = df.cols.infer_dtypes(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['null', 'null', 'null', 'null', 'null', 'null'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_infer_dtypes_numeric_float(self):
        df = self.df
        result = df.cols.infer_dtypes(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['int', 'int', 'int', 'int', 'null', 'int'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_infer_dtypes_numeric_int(self):
        df = self.df
        result = df.cols.infer_dtypes(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['int', 'int', 'int', 'int', 'int', 'int'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_infer_dtypes_string(self):
        df = self.df
        result = df.cols.infer_dtypes(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['str', 'str', 'str', 'str', 'str', 'str'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lemmatize_verbs_all(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lemmatize_verbs_bool(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lemmatize_verbs_bytearray(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lemmatize_verbs_datetime(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lemmatize_verbs_list(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lemmatize_verbs_multiple(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_lemmatize_verbs_NoneType(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lemmatize_verbs_numeric_float(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lemmatize_verbs_numeric_int(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lemmatize_verbs_string(self):
        df = self.df
        result = df.cols.lemmatize_verbs(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_len_all(self):
        df = self.df
        result = df.cols.len(cols='*')
        result = result.to_dict()
        expected = {'NullType': [4, 4, 4, 4, 4, 4], 'attributes': [16, 15, 16, 16, 14, 13], 'date arrival': [10, 10, 10, 10, 10, 10], 'function(binary)': [20, 23, 22, 30, 18, 28], 'height(ft)': [5, 4, 4, 4, 3, 5], 'japanese name': [20, 22, 14, 11, 12, 13], 'last date seen': [10, 10, 10, 10, 10, 10], 'last position seen': [20, 20, 21, 21, 4, 4], 'rank': [2, 1, 1, 1, 2, 1], 'Cybertronian': [4, 4, 4, 4, 4, 5], 'Date Type': [10, 10, 10, 10, 10, 10], 'age': [7, 7, 7, 7, 7, 7], 'function': [6, 9, 8, 16, 4, 14], 'names': [7, 12, 9, 4, 8, 13], 'timestamp': [10, 10, 10, 10, 10, 10], 'weight(t)': [3, 3, 3, 3, 3, 3]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_len_bool(self):
        df = self.df
        result = df.cols.len(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [4, 4, 4, 4, 4, 5],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_len_bytearray(self):
        df = self.df
        result = df.cols.len(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [20, 23, 22, 30, 18, 28],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_len_datetime(self):
        df = self.df
        result = df.cols.len(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [10, 10, 10, 10, 10, 10],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_len_list(self):
        df = self.df
        result = df.cols.len(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [16, 15, 16, 16, 14, 13],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_len_multiple(self):
        df = self.df
        result = df.cols.len(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': [4, 4, 4, 4, 4, 4],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': [10, 10, 10, 10, 10, 10],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [20, 22, 14, 11, 12, 13],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': [20, 20, 21, 21, 4, 4],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [10, 10, 10, 10, 10, 10],
 'age': [7, 7, 7, 7, 7, 7],
 'function': [6, 9, 8, 16, 4, 14],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [10, 10, 10, 10, 10, 10],
 'weight(t)': [3, 3, 3, 3, 3, 3]}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_len_NoneType(self):
        df = self.df
        result = df.cols.len(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': [4, 4, 4, 4, 4, 4],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_len_numeric_float(self):
        df = self.df
        result = df.cols.len(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [5, 4, 4, 4, 3, 5],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_len_numeric_int(self):
        df = self.df
        result = df.cols.len(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [2, 1, 1, 1, 2, 1],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_len_string(self):
        df = self.df
        result = df.cols.len(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [7, 12, 9, 4, 8, 13],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ln_all(self):
        df = self.df
        result = df.cols.ln(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ln_bool(self):
        df = self.df
        result = df.cols.ln(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.0, 0.0, 0.0, 0.0, 0.0, -inf],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ln_bytearray(self):
        df = self.df
        result = df.cols.ln(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ln_datetime(self):
        df = self.df
        result = df.cols.ln(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ln_list(self):
        df = self.df
        result = df.cols.ln(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ln_multiple(self):
        df = self.df
        result = df.cols.ln(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_ln_NoneType(self):
        df = self.df
        result = df.cols.ln(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ln_numeric_float(self):
        df = self.df
        result = df.cols.ln(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [nan, 2.833213344056216, 3.258096538021482, 2.5649493574615367, nan, 5.703782474656201],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ln_numeric_int(self):
        df = self.df
        result = df.cols.ln(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [2.302585092994046, 1.9459101490553132, 1.9459101490553132, 2.0794415416798357, 2.302585092994046, 2.0794415416798357],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_ln_string(self):
        df = self.df
        result = df.cols.ln(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lower_all(self):
        df = self.df
        result = df.cols.lower(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['none', 'none', 'none', 'none', 'none', 'none'],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[none, 5700.0]', '[91.44, none]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'leader')", "bytearray(b'espionage')", "bytearray(b'security')", "bytearray(b'first lieutenant')", "bytearray(b'none')", "bytearray(b'battle station')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': ["['inochi', 'convoy']", "['bumble', 'goldback']", "['roadbuster']", "['meister']", "['megatron']", "['metroflex']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'none', 'none'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['true', 'true', 'true', 'true', 'true', 'false'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['leader', 'espionage', 'security', 'first lieutenant', 'none', 'battle station'],
 'names': ['optimus', 'bumbl#ebéé  ', 'ironhide&', 'jazz', 'megatron', 'metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lower_bool(self):
        df = self.df
        result = df.cols.lower(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['true', 'true', 'true', 'true', 'true', 'false'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lower_bytearray(self):
        df = self.df
        result = df.cols.lower(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'leader')", "bytearray(b'espionage')", "bytearray(b'security')", "bytearray(b'first lieutenant')", "bytearray(b'none')", "bytearray(b'battle station')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lower_datetime(self):
        df = self.df
        result = df.cols.lower(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lower_list(self):
        df = self.df
        result = df.cols.lower(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[none, 5700.0]', '[91.44, none]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lower_multiple(self):
        df = self.df
        result = df.cols.lower(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['none', 'none', 'none', 'none', 'none', 'none'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['inochi', 'convoy']", "['bumble', 'goldback']", "['roadbuster']", "['meister']", "['megatron']", "['metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'none', 'none'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['leader', 'espionage', 'security', 'first lieutenant', 'none', 'battle station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_lower_NoneType(self):
        df = self.df
        result = df.cols.lower(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['none', 'none', 'none', 'none', 'none', 'none'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lower_numeric_float(self):
        df = self.df
        result = df.cols.lower(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lower_numeric_int(self):
        df = self.df
        result = df.cols.lower(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_lower_string(self):
        df = self.df
        result = df.cols.lower(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['optimus', 'bumbl#ebéé  ', 'ironhide&', 'jazz', 'megatron', 'metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_match_rating_codex_all(self):
        df = self.df
        result = df.cols.match_rating_codex(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_match_rating_codex_bool(self):
        df = self.df
        result = df.cols.match_rating_codex(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_match_rating_codex_bytearray(self):
        df = self.df
        result = df.cols.match_rating_codex(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_match_rating_codex_datetime(self):
        df = self.df
        result = df.cols.match_rating_codex(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_match_rating_codex_list(self):
        df = self.df
        result = df.cols.match_rating_codex(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_match_rating_codex_multiple(self):
        df = self.df
        result = df.cols.match_rating_codex(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_match_rating_codex_NoneType(self):
        df = self.df
        result = df.cols.match_rating_codex(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_match_rating_codex_numeric_float(self):
        df = self.df
        result = df.cols.match_rating_codex(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_match_rating_codex_numeric_int(self):
        df = self.df
        result = df.cols.match_rating_codex(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_match_rating_codex_string(self):
        df = self.df
        result = df.cols.match_rating_codex(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['OPTMS', 'BMBBÉ ', 'IRNHD&', 'JZ', 'MGTRN', 'MTR)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_max_abs_scaler_all(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_max_abs_scaler_bool(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.0, 1.0, 1.0, 1.0, 1.0, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_max_abs_scaler_bytearray(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_max_abs_scaler_datetime(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [1.0, 0.9767210038700598, 0.9535006450099683, 0.9303389234197256, 0.9071185645596341, 0.8838982056995426],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_max_abs_scaler_list(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_max_abs_scaler_multiple(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_max_abs_scaler_NoneType(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': [nan, nan, nan, nan, nan, nan],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_max_abs_scaler_numeric_float(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-0.09333333333333334, 0.056666666666666664, 0.08666666666666667, 0.043333333333333335, nan, 1.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_max_abs_scaler_numeric_int(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [1.0, 0.7, 0.7, 0.8, 1.0, 0.8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_max_abs_scaler_string(self):
        df = self.df
        result = df.cols.max_abs_scaler(cols=['names'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_metaphone_all(self):
        df = self.df
        result = df.cols.metaphone(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_metaphone_bool(self):
        df = self.df
        result = df.cols.metaphone(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_metaphone_bytearray(self):
        df = self.df
        result = df.cols.metaphone(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_metaphone_datetime(self):
        df = self.df
        result = df.cols.metaphone(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_metaphone_list(self):
        df = self.df
        result = df.cols.metaphone(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_metaphone_multiple(self):
        df = self.df
        result = df.cols.metaphone(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_metaphone_NoneType(self):
        df = self.df
        result = df.cols.metaphone(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_metaphone_numeric_float(self):
        df = self.df
        result = df.cols.metaphone(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_metaphone_numeric_int(self):
        df = self.df
        result = df.cols.metaphone(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_metaphone_string(self):
        df = self.df
        result = df.cols.metaphone(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['OPTMS', 'BMBLB ', 'IRNHT', 'JS', 'MKTRN', 'MTRPLKS'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_min_max_scaler_all(self):
        df = self.df
        result = df.cols.min_max_scaler(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_min_max_scaler_bool(self):
        df = self.df
        result = df.cols.min_max_scaler(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.0, 1.0, 1.0, 1.0, 1.0, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_min_max_scaler_bytearray(self):
        df = self.df
        result = df.cols.min_max_scaler(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_min_max_scaler_datetime(self):
        df = self.df
        result = df.cols.min_max_scaler(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [1.0000000000000009, 0.7994949494949504, 0.5994949494949493, 0.39999999999999947, 0.20000000000000018, 0.0],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_min_max_scaler_list(self):
        df = self.df
        result = df.cols.min_max_scaler(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_min_max_scaler_multiple(self):
        df = self.df
        result = df.cols.min_max_scaler(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_min_max_scaler_NoneType(self):
        df = self.df
        result = df.cols.min_max_scaler(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': [nan, nan, nan, nan, nan, nan],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_min_max_scaler_numeric_float(self):
        df = self.df
        result = df.cols.min_max_scaler(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [0.0, 0.13719512195121952, 0.16463414634146342, 0.125, nan, 1.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_min_max_scaler_numeric_int(self):
        df = self.df
        result = df.cols.min_max_scaler(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [1.0, 0.0, 0.0, 0.3333333333333335, 1.0, 0.3333333333333335],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_min_max_scaler_string(self):
        df = self.df
        result = df.cols.min_max_scaler(cols=['names'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_modified_z_score_all(self):
        df = self.df
        result = df.cols.modified_z_score(cols='*')
        result = result.to_dict()
        expected = {'NullType': [nan, nan, nan, nan, nan, nan],
 'attributes': [nan, nan, nan, nan, nan, nan],
 'date arrival': [nan, nan, nan, nan, nan, nan],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [3.3725, 0.0, 0.6745, 0.29977777777777775, nan, 21.20927777777778],
 'japanese name': [nan, nan, nan, nan, nan, nan],
 'last date seen': [1.1256819713563606, 0.6745, 0.22445450716090987, 0.22445450716090987, 0.6745, 1.12454549283909],
 'last position seen': [nan, nan, nan, nan, nan, nan],
 'rank': [1.349, 0.6745, 0.6745, 0.0, 1.349, 0.0],
 'Cybertronian': [nan, nan, nan, nan, nan, inf],
 'Date Type': [1.1268184498736311, 0.6756364785172704, 0.2074073294018534, 0.2074073294018534, 0.6733635214827296, 1.1234090143218196],
 'age': [nan, nan, nan, nan, nan, nan],
 'function': [nan, nan, nan, nan, nan, nan],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [nan, nan, nan, nan, nan, nan],
 'weight(t)': [0.11902941176470579, 0.7935294117647058, 0.0, 0.8728823529411763, 0.6745, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_modified_z_score_bool(self):
        df = self.df
        result = df.cols.modified_z_score(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [nan, nan, nan, nan, nan, inf],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_modified_z_score_bytearray(self):
        df = self.df
        result = df.cols.modified_z_score(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_modified_z_score_datetime(self):
        df = self.df
        result = df.cols.modified_z_score(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [1.1256819713563606, 0.6745, 0.22445450716090987, 0.22445450716090987, 0.6745, 1.12454549283909],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_modified_z_score_list(self):
        df = self.df
        result = df.cols.modified_z_score(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [nan, nan, nan, nan, nan, nan],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_modified_z_score_multiple(self):
        df = self.df
        result = df.cols.modified_z_score(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': [nan, nan, nan, nan, nan, nan],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': [nan, nan, nan, nan, nan, nan],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [nan, nan, nan, nan, nan, nan],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': [nan, nan, nan, nan, nan, nan],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [1.1268184498736311, 0.6756364785172704, 0.2074073294018534, 0.2074073294018534, 0.6733635214827296, 1.1234090143218196],
 'age': [nan, nan, nan, nan, nan, nan],
 'function': [nan, nan, nan, nan, nan, nan],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [nan, nan, nan, nan, nan, nan],
 'weight(t)': [0.11902941176470579, 0.7935294117647058, 0.0, 0.8728823529411763, 0.6745, nan]}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_modified_z_score_NoneType(self):
        df = self.df
        result = df.cols.modified_z_score(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': [nan, nan, nan, nan, nan, nan],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_modified_z_score_numeric_float(self):
        df = self.df
        result = df.cols.modified_z_score(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [3.3725, 0.0, 0.6745, 0.29977777777777775, nan, 21.20927777777778],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_modified_z_score_numeric_int(self):
        df = self.df
        result = df.cols.modified_z_score(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [1.349, 0.6745, 0.6745, 0.0, 1.349, 0.0],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_modified_z_score_string(self):
        df = self.df
        result = df.cols.modified_z_score(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_normalize_spaces_all(self):
        df = self.df
        result = df.cols.normalize_spaces(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_normalize_spaces_bool(self):
        df = self.df
        result = df.cols.normalize_spaces(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_normalize_spaces_bytearray(self):
        df = self.df
        result = df.cols.normalize_spaces(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_normalize_spaces_datetime(self):
        df = self.df
        result = df.cols.normalize_spaces(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_normalize_spaces_list(self):
        df = self.df
        result = df.cols.normalize_spaces(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_normalize_spaces_multiple(self):
        df = self.df
        result = df.cols.normalize_spaces(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_normalize_spaces_NoneType(self):
        df = self.df
        result = df.cols.normalize_spaces(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_normalize_spaces_numeric_float(self):
        df = self.df
        result = df.cols.normalize_spaces(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_normalize_spaces_numeric_int(self):
        df = self.df
        result = df.cols.normalize_spaces(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_normalize_spaces_string(self):
        df = self.df
        result = df.cols.normalize_spaces(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_nysiis_all(self):
        df = self.df
        result = df.cols.nysiis(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_nysiis_bool(self):
        df = self.df
        result = df.cols.nysiis(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_nysiis_bytearray(self):
        df = self.df
        result = df.cols.nysiis(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_nysiis_datetime(self):
        df = self.df
        result = df.cols.nysiis(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_nysiis_list(self):
        df = self.df
        result = df.cols.nysiis(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_nysiis_multiple(self):
        df = self.df
        result = df.cols.nysiis(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_nysiis_NoneType(self):
        df = self.df
        result = df.cols.nysiis(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_nysiis_numeric_float(self):
        df = self.df
        result = df.cols.nysiis(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_nysiis_numeric_int(self):
        df = self.df
        result = df.cols.nysiis(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_nysiis_string(self):
        df = self.df
        result = df.cols.nysiis(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['OPTAN', 'BANBL#ABÉ ', 'IRANADA&', 'J', 'MAGATRAN', 'MATRAPLAX_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_port_all(self):
        df = self.df
        result = df.cols.port(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_port_bool(self):
        df = self.df
        result = df.cols.port(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_port_bytearray(self):
        df = self.df
        result = df.cols.port(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_port_datetime(self):
        df = self.df
        result = df.cols.port(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_port_list(self):
        df = self.df
        result = df.cols.port(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_port_multiple(self):
        df = self.df
        result = df.cols.port(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_port_NoneType(self):
        df = self.df
        result = df.cols.port(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_port_numeric_float(self):
        df = self.df
        result = df.cols.port(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_port_numeric_int(self):
        df = self.df
        result = df.cols.port(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_port_string(self):
        df = self.df
        result = df.cols.port(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [None, None, None, None, None, None],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_pos_all(self):
        df = self.df
        result = df.cols.pos(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_pos_bool(self):
        df = self.df
        result = df.cols.pos(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_pos_bytearray(self):
        df = self.df
        result = df.cols.pos(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_pos_datetime(self):
        df = self.df
        result = df.cols.pos(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_pos_list(self):
        df = self.df
        result = df.cols.pos(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_pos_multiple(self):
        df = self.df
        result = df.cols.pos(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_pos_NoneType(self):
        df = self.df
        result = df.cols.pos(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_pos_numeric_float(self):
        df = self.df
        result = df.cols.pos(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_pos_numeric_int(self):
        df = self.df
        result = df.cols.pos(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_pos_string(self):
        df = self.df
        result = df.cols.pos(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [[('Optimus', 'NN')], [('bumbl#ebéé', 'NN')], [('ironhide&', 'NN')], [('Jazz', 'NN')], [('Megatron', 'NNP')], [('Metroplex_)^$', 'NN')]],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_proper_all(self):
        df = self.df
        result = df.cols.proper(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["Bytearray(B'Leader')", "Bytearray(B'Espionage')", "Bytearray(B'Security')", "Bytearray(B'First Lieutenant')", "Bytearray(B'None')", "Bytearray(B'Battle Station')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0'],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'Bumbl#Ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_proper_bool(self):
        df = self.df
        result = df.cols.proper(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_proper_bytearray(self):
        df = self.df
        result = df.cols.proper(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["Bytearray(B'Leader')", "Bytearray(B'Espionage')", "Bytearray(B'Security')", "Bytearray(B'First Lieutenant')", "Bytearray(B'None')", "Bytearray(B'Battle Station')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_proper_datetime(self):
        df = self.df
        result = df.cols.proper(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_proper_list(self):
        df = self.df
        result = df.cols.proper(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_proper_multiple(self):
        df = self.df
        result = df.cols.proper(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_proper_NoneType(self):
        df = self.df
        result = df.cols.proper(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_proper_numeric_float(self):
        df = self.df
        result = df.cols.proper(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_proper_numeric_int(self):
        df = self.df
        result = df.cols.proper(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_proper_string(self):
        df = self.df
        result = df.cols.proper(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'Bumbl#Ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reciprocal_all(self):
        df = self.df
        result = df.cols.reciprocal(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reciprocal_bool(self):
        df = self.df
        result = df.cols.reciprocal(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.0, 1.0, 1.0, 1.0, 1.0, inf],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reciprocal_bytearray(self):
        df = self.df
        result = df.cols.reciprocal(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reciprocal_datetime(self):
        df = self.df
        result = df.cols.reciprocal(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reciprocal_list(self):
        df = self.df
        result = df.cols.reciprocal(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reciprocal_multiple(self):
        df = self.df
        result = df.cols.reciprocal(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_reciprocal_NoneType(self):
        df = self.df
        result = df.cols.reciprocal(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reciprocal_numeric_float(self):
        df = self.df
        result = df.cols.reciprocal(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-0.03571428571428571, 0.058823529411764705, 0.038461538461538464, 0.07692307692307693, nan, 0.0033333333333333335],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reciprocal_numeric_int(self):
        df = self.df
        result = df.cols.reciprocal(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [0.1, 0.14285714285714285, 0.14285714285714285, 0.125, 0.1, 0.125],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reciprocal_string(self):
        df = self.df
        result = df.cols.reciprocal(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_numbers_all(self):
        df = self.df
        result = df.cols.remove_numbers(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['[., .]', '[., .]', '[., .]', '[., .]', '[None, .]', '[., None]'],
 'date arrival': ['//', '//', '//', '//', '//', '//'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': ['-.', '.', '.', '.', 'nan', '.'],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': ['--', '--', '--', '--', '--', '--'],
 'last position seen': ['.,-.', '.,-.', '.,-.', '.,-.', 'None', 'None'],
 'rank': ['', '', '', '', '', ''],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['--', '--', '--', '--', '--', '--'],
 'age': ['', '', '', '', '', ''],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['--', '--', '--', '--', '--', '--'],
 'weight(t)': ['.', '.', '.', '.', '.', 'nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_numbers_bool(self):
        df = self.df
        result = df.cols.remove_numbers(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_numbers_bytearray(self):
        df = self.df
        result = df.cols.remove_numbers(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_numbers_datetime(self):
        df = self.df
        result = df.cols.remove_numbers(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['--', '--', '--', '--', '--', '--'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_numbers_list(self):
        df = self.df
        result = df.cols.remove_numbers(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[., .]', '[., .]', '[., .]', '[., .]', '[None, .]', '[., None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_numbers_multiple(self):
        df = self.df
        result = df.cols.remove_numbers(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['//', '//', '//', '//', '//', '//'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['.,-.', '.,-.', '.,-.', '.,-.', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['--', '--', '--', '--', '--', '--'],
 'age': ['', '', '', '', '', ''],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['--', '--', '--', '--', '--', '--'],
 'weight(t)': ['.', '.', '.', '.', '.', 'nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_remove_numbers_NoneType(self):
        df = self.df
        result = df.cols.remove_numbers(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_numbers_numeric_float(self):
        df = self.df
        result = df.cols.remove_numbers(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-.', '.', '.', '.', 'nan', '.'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_numbers_numeric_int(self):
        df = self.df
        result = df.cols.remove_numbers(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['', '', '', '', '', ''],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_numbers_string(self):
        df = self.df
        result = df.cols.remove_numbers(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_special_chars_all(self):
        df = self.df
        result = df.cols.remove_special_chars(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['85344 43000', '5334 20000', '79248 40000', '39624 18000', 'None 57000', '9144 None'],
 'date arrival': ['19800410', '19800410', '19800410', '19800410', '19800410', '19800410'],
 'function(binary)': ['bytearraybLeader', 'bytearraybEspionage', 'bytearraybSecurity', 'bytearraybFirst Lieutenant', 'bytearraybNone', 'bytearraybBattle Station'],
 'height(ft)': ['280', '170', '260', '130', 'nan', '3000'],
 'japanese name': ['Inochi Convoy', 'Bumble Goldback', 'Roadbuster', 'Meister', 'Megatron', 'Metroflex'],
 'last date seen': ['20160910', '20150810', '20140710', '20130610', '20120510', '20110410'],
 'last position seen': ['1944273599201111', '1064270771612534', '37789563122400356', '33670666117841553', 'None', 'None'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['20160910', '20150810', '20140624', '20130624', '20120510', '20110410'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumblebéé  ', 'ironhide', 'Jazz', 'Megatron', 'Metroplex'],
 'timestamp': ['20140624', '20140624', '20140624', '20140624', '20140624', '20140624'],
 'weight(t)': ['43', '20', '40', '18', '57', 'nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_special_chars_bool(self):
        df = self.df
        result = df.cols.remove_special_chars(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_special_chars_bytearray(self):
        df = self.df
        result = df.cols.remove_special_chars(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ['bytearraybLeader', 'bytearraybEspionage', 'bytearraybSecurity', 'bytearraybFirst Lieutenant', 'bytearraybNone', 'bytearraybBattle Station'],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_special_chars_datetime(self):
        df = self.df
        result = df.cols.remove_special_chars(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['20160910', '20150810', '20140710', '20130610', '20120510', '20110410'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_special_chars_list(self):
        df = self.df
        result = df.cols.remove_special_chars(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['85344 43000', '5334 20000', '79248 40000', '39624 18000', 'None 57000', '9144 None'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_special_chars_multiple(self):
        df = self.df
        result = df.cols.remove_special_chars(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['19800410', '19800410', '19800410', '19800410', '19800410', '19800410'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ['Inochi Convoy', 'Bumble Goldback', 'Roadbuster', 'Meister', 'Megatron', 'Metroflex'],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['1944273599201111', '1064270771612534', '37789563122400356', '33670666117841553', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['20160910', '20150810', '20140624', '20130624', '20120510', '20110410'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['20140624', '20140624', '20140624', '20140624', '20140624', '20140624'],
 'weight(t)': ['43', '20', '40', '18', '57', 'nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_remove_special_chars_NoneType(self):
        df = self.df
        result = df.cols.remove_special_chars(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_special_chars_numeric_float(self):
        df = self.df
        result = df.cols.remove_special_chars(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['280', '170', '260', '130', 'nan', '3000'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_special_chars_numeric_int(self):
        df = self.df
        result = df.cols.remove_special_chars(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_special_chars_string(self):
        df = self.df
        result = df.cols.remove_special_chars(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumblebéé  ', 'ironhide', 'Jazz', 'Megatron', 'Metroplex'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_urls_all(self):
        df = self.df
        result = df.cols.remove_urls(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_urls_bool(self):
        df = self.df
        result = df.cols.remove_urls(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_urls_bytearray(self):
        df = self.df
        result = df.cols.remove_urls(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_urls_datetime(self):
        df = self.df
        result = df.cols.remove_urls(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_urls_list(self):
        df = self.df
        result = df.cols.remove_urls(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_urls_multiple(self):
        df = self.df
        result = df.cols.remove_urls(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_remove_urls_NoneType(self):
        df = self.df
        result = df.cols.remove_urls(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_urls_numeric_float(self):
        df = self.df
        result = df.cols.remove_urls(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_urls_numeric_int(self):
        df = self.df
        result = df.cols.remove_urls(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_urls_string(self):
        df = self.df
        result = df.cols.remove_urls(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_white_spaces_all(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['[8.5344,4300.0]', '[5.334,2000.0]', '[7.9248,4000.0]', '[3.9624,1800.0]', '[None,5700.0]', '[91.44,None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'FirstLieutenant')", "bytearray(b'None')", "bytearray(b'BattleStation')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': ["['Inochi','Convoy']", "['Bumble','Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'FirstLieutenant', 'None', 'BattleStation'],
 'names': ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_white_spaces_bool(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_white_spaces_bytearray(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'FirstLieutenant')", "bytearray(b'None')", "bytearray(b'BattleStation')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_white_spaces_datetime(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_white_spaces_list(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344,4300.0]', '[5.334,2000.0]', '[7.9248,4000.0]', '[3.9624,1800.0]', '[None,5700.0]', '[91.44,None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_white_spaces_multiple(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['Inochi','Convoy']", "['Bumble','Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'FirstLieutenant', 'None', 'BattleStation'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_remove_white_spaces_NoneType(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_white_spaces_numeric_float(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_white_spaces_numeric_int(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_remove_white_spaces_string(self):
        df = self.df
        result = df.cols.remove_white_spaces(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reverse_all(self):
        df = self.df
        result = df.cols.reverse(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['enoN', 'enoN', 'enoN', 'enoN', 'enoN', 'enoN'],
 'attributes': [']0.0034 ,4435.8[', ']0.0002 ,433.5[', ']0.0004 ,8429.7[', ']0.0081 ,4269.3[', ']0.0075 ,enoN[', ']enoN ,44.19['],
 'date arrival': ['01/40/0891', '01/40/0891', '01/40/0891', '01/40/0891', '01/40/0891', '01/40/0891'],
 'function(binary)': [")'redaeL'b(yarraetyb", ")'eganoipsE'b(yarraetyb", ")'ytiruceS'b(yarraetyb", ")'tnanetueiL tsriF'b(yarraetyb", ")'enoN'b(yarraetyb", ")'noitatS elttaB'b(yarraetyb"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["]'yovnoC' ,'ihconI'[", "]'kcabdloG' ,'elbmuB'[", "]'retsubdaoR'[", "]'retsieM'[", "]'nortageM'[", "]'xelforteM'["],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['111102.99-,537244.91', '435216.17-,707246.01', '653004.221-,365987.73', '355148.711-,666076.33', 'enoN', 'enoN'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['redaeL', 'eganoipsE', 'ytiruceS', 'tnanetueiL tsriF', 'enoN', 'noitatS elttaB'],
 'names': ['sumitpO', '  éébe#lbmub', '&edihnori', 'zzaJ', 'nortageM', '$^)_xelporteM'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reverse_bool(self):
        df = self.df
        result = df.cols.reverse(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reverse_bytearray(self):
        df = self.df
        result = df.cols.reverse(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [")'redaeL'b(yarraetyb", ")'eganoipsE'b(yarraetyb", ")'ytiruceS'b(yarraetyb", ")'tnanetueiL tsriF'b(yarraetyb", ")'enoN'b(yarraetyb", ")'noitatS elttaB'b(yarraetyb"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reverse_datetime(self):
        df = self.df
        result = df.cols.reverse(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reverse_list(self):
        df = self.df
        result = df.cols.reverse(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [']0.0034 ,4435.8[', ']0.0002 ,433.5[', ']0.0004 ,8429.7[', ']0.0081 ,4269.3[', ']0.0075 ,enoN[', ']enoN ,44.19['],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reverse_multiple(self):
        df = self.df
        result = df.cols.reverse(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['enoN', 'enoN', 'enoN', 'enoN', 'enoN', 'enoN'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['01/40/0891', '01/40/0891', '01/40/0891', '01/40/0891', '01/40/0891', '01/40/0891'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["]'yovnoC' ,'ihconI'[", "]'kcabdloG' ,'elbmuB'[", "]'retsubdaoR'[", "]'retsieM'[", "]'nortageM'[", "]'xelforteM'["],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['111102.99-,537244.91', '435216.17-,707246.01', '653004.221-,365987.73', '355148.711-,666076.33', 'enoN', 'enoN'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['redaeL', 'eganoipsE', 'ytiruceS', 'tnanetueiL tsriF', 'enoN', 'noitatS elttaB'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_reverse_NoneType(self):
        df = self.df
        result = df.cols.reverse(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['enoN', 'enoN', 'enoN', 'enoN', 'enoN', 'enoN'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reverse_numeric_float(self):
        df = self.df
        result = df.cols.reverse(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reverse_numeric_int(self):
        df = self.df
        result = df.cols.reverse(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_reverse_string(self):
        df = self.df
        result = df.cols.reverse(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['sumitpO', '  éébe#lbmub', '&edihnori', 'zzaJ', 'nortageM', '$^)_xelporteM'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sinh_all(self):
        df = self.df
        result = df.cols.sinh(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sinh_bool(self):
        df = self.df
        result = df.cols.sinh(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.881373587019543, 0.881373587019543, 0.881373587019543, 0.881373587019543, 0.881373587019543, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sinh_bytearray(self):
        df = self.df
        result = df.cols.sinh(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sinh_datetime(self):
        df = self.df
        result = df.cols.sinh(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sinh_list(self):
        df = self.df
        result = df.cols.sinh(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sinh_multiple(self):
        df = self.df
        result = df.cols.sinh(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_sinh_NoneType(self):
        df = self.df
        result = df.cols.sinh(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sinh_numeric_float(self):
        df = self.df
        result = df.cols.sinh(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-4.025670415869822, 3.5272244561999657, 3.9516133360820653, 3.2595725562629214, nan, 6.39693243298235],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sinh_numeric_int(self):
        df = self.df
        result = df.cols.sinh(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [2.99822295029797, 2.644120761058629, 2.644120761058629, 2.7764722807237177, 2.99822295029797, 2.7764722807237177],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sinh_string(self):
        df = self.df
        result = df.cols.sinh(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sin_all(self):
        df = self.df
        result = df.cols.sin(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sin_bool(self):
        df = self.df
        result = df.cols.sin(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.8414709848078965, 0.8414709848078965, 0.8414709848078965, 0.8414709848078965, 0.8414709848078965, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sin_bytearray(self):
        df = self.df
        result = df.cols.sin(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sin_datetime(self):
        df = self.df
        result = df.cols.sin(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sin_list(self):
        df = self.df
        result = df.cols.sin(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sin_multiple(self):
        df = self.df
        result = df.cols.sin(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_sin_NoneType(self):
        df = self.df
        result = df.cols.sin(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sin_numeric_float(self):
        df = self.df
        result = df.cols.sin(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-0.27090578830786904, -0.9613974918795568, 0.7625584504796027, 0.4201670368266409, nan, -0.9997558399011495],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sin_numeric_int(self):
        df = self.df
        result = df.cols.sin(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [-0.5440211108893698, 0.6569865987187891, 0.6569865987187891, 0.9893582466233818, -0.5440211108893698, 0.9893582466233818],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sin_string(self):
        df = self.df
        result = df.cols.sin(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_soundex_all(self):
        df = self.df
        result = df.cols.soundex(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_soundex_bool(self):
        df = self.df
        result = df.cols.soundex(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_soundex_bytearray(self):
        df = self.df
        result = df.cols.soundex(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_soundex_datetime(self):
        df = self.df
        result = df.cols.soundex(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_soundex_list(self):
        df = self.df
        result = df.cols.soundex(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_soundex_multiple(self):
        df = self.df
        result = df.cols.soundex(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_soundex_NoneType(self):
        df = self.df
        result = df.cols.soundex(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_soundex_numeric_float(self):
        df = self.df
        result = df.cols.soundex(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_soundex_numeric_int(self):
        df = self.df
        result = df.cols.soundex(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_soundex_string(self):
        df = self.df
        result = df.cols.soundex(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['O135', 'B514', 'I653', 'J200', 'M236', 'M361'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sqrt_all(self):
        df = self.df
        result = df.cols.sqrt(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sqrt_bool(self):
        df = self.df
        result = df.cols.sqrt(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.0, 1.0, 1.0, 1.0, 1.0, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sqrt_bytearray(self):
        df = self.df
        result = df.cols.sqrt(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sqrt_datetime(self):
        df = self.df
        result = df.cols.sqrt(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sqrt_list(self):
        df = self.df
        result = df.cols.sqrt(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sqrt_multiple(self):
        df = self.df
        result = df.cols.sqrt(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_sqrt_NoneType(self):
        df = self.df
        result = df.cols.sqrt(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sqrt_numeric_float(self):
        df = self.df
        result = df.cols.sqrt(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [nan, 4.123105625617661, 5.0990195135927845, 3.605551275463989, nan, 17.320508075688775],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sqrt_numeric_int(self):
        df = self.df
        result = df.cols.sqrt(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [3.1622776601683795, 2.6457513110645907, 2.6457513110645907, 2.8284271247461903, 3.1622776601683795, 2.8284271247461903],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sqrt_string(self):
        df = self.df
        result = df.cols.sqrt(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_standard_scaler_all(self):
        df = self.df
        result = df.cols.standard_scaler(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_standard_scaler_bool(self):
        df = self.df
        result = df.cols.standard_scaler(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.4472135954999578, 0.4472135954999578, 0.4472135954999578, 0.4472135954999578, 0.4472135954999578, -2.23606797749979],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_standard_scaler_bytearray(self):
        df = self.df
        result = df.cols.standard_scaler(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_standard_scaler_datetime(self):
        df = self.df
        result = df.cols.standard_scaler(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [1.4647654250523028, 0.8775774004015816, 0.2918684387852451, -0.2923614597967067, -0.8780704214130431, -1.4637793830293797],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_standard_scaler_list(self):
        df = self.df
        result = df.cols.standard_scaler(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_standard_scaler_multiple(self):
        df = self.df
        result = df.cols.standard_scaler(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_standard_scaler_NoneType(self):
        df = self.df
        result = df.cols.standard_scaler(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': [nan, nan, nan, nan, nan, nan],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_standard_scaler_numeric_float(self):
        df = self.df
        result = df.cols.standard_scaler(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-0.7888071163227179, -0.4095729257829497, -0.333726087674996, -0.44328263160870685, nan, 1.9753887613893708],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_standard_scaler_numeric_int(self):
        df = self.df
        result = df.cols.standard_scaler(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [1.3363062095621216, -1.069044967649698, -1.069044967649698, -0.2672612419124249, 1.3363062095621216, -0.2672612419124249],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_standard_scaler_string(self):
        df = self.df
        result = df.cols.standard_scaler(cols=['names'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_string_to_index_all(self):
        df = self.df
        result = df.cols.string_to_index(cols='*')
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'NullType_string_to_index': [0, 0, 0, 0, 0, 0],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'attributes_string_to_index': [3, 1, 2, 0, 5, 4],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'date arrival_string_to_index': [0, 0, 0, 0, 0, 0],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'function(binary)_string_to_index': [3, 1, 5, 2, 4, 0],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'height(ft)_string_to_index': [0, 2, 3, 1, 5, 4],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'japanese name_string_to_index': [1, 0, 5, 3, 2, 4],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last date seen_string_to_index': [5, 4, 3, 2, 1, 0],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'last position seen_string_to_index': [1, 0, 3, 2, 4, 4],
 'rank': [10, 7, 7, 8, 10, 8],
 'rank_string_to_index': [0, 1, 1, 2, 0, 2],
 'Cybertronian': [True, True, True, True, True, False],
 'Cybertronian_string_to_index': [1, 1, 1, 1, 1, 0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'Date Type_string_to_index': [5, 4, 3, 2, 1, 0],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'age_string_to_index': [0, 0, 0, 0, 0, 0],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'function_string_to_index': [3, 1, 5, 2, 4, 0],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'names_string_to_index': [3, 4, 5, 0, 1, 2],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'timestamp_string_to_index': [0, 0, 0, 0, 0, 0],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan],
 'weight(t)_string_to_index': [3, 1, 2, 0, 4, 5]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_string_to_index_bool(self):
        df = self.df
        result = df.cols.string_to_index(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Cybertronian_string_to_index': [1, 1, 1, 1, 1, 0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_string_to_index_bytearray(self):
        df = self.df
        result = df.cols.string_to_index(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'function(binary)_string_to_index': [3, 1, 5, 2, 4, 0],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_string_to_index_datetime(self):
        df = self.df
        result = df.cols.string_to_index(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last date seen_string_to_index': [5, 4, 3, 2, 1, 0],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_string_to_index_list(self):
        df = self.df
        result = df.cols.string_to_index(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'attributes_string_to_index': [3, 1, 2, 0, 5, 4],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_string_to_index_multiple(self):
        df = self.df
        result = df.cols.string_to_index(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'NullType_string_to_index': [0, 0, 0, 0, 0, 0],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'date arrival_string_to_index': [0, 0, 0, 0, 0, 0],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'japanese name_string_to_index': [1, 0, 5, 3, 2, 4],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'last position seen_string_to_index': [1, 0, 3, 2, 4, 4],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'Date Type_string_to_index': [5, 4, 3, 2, 1, 0],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'age_string_to_index': [0, 0, 0, 0, 0, 0],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'function_string_to_index': [3, 1, 5, 2, 4, 0],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'timestamp_string_to_index': [0, 0, 0, 0, 0, 0],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan],
 'weight(t)_string_to_index': [3, 1, 2, 0, 4, 5]}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_string_to_index_NoneType(self):
        df = self.df
        result = df.cols.string_to_index(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'NullType_string_to_index': [0, 0, 0, 0, 0, 0],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_string_to_index_numeric_float(self):
        df = self.df
        result = df.cols.string_to_index(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'height(ft)_string_to_index': [0, 2, 3, 1, 5, 4],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_string_to_index_numeric_int(self):
        df = self.df
        result = df.cols.string_to_index(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'rank_string_to_index': [0, 1, 1, 2, 0, 2],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_string_to_index_string(self):
        df = self.df
        result = df.cols.string_to_index(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'names_string_to_index': [3, 4, 5, 0, 1, 2],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_strip_html_all(self):
        df = self.df
        result = df.cols.strip_html(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_strip_html_bool(self):
        df = self.df
        result = df.cols.strip_html(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_strip_html_bytearray(self):
        df = self.df
        result = df.cols.strip_html(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_strip_html_datetime(self):
        df = self.df
        result = df.cols.strip_html(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_strip_html_list(self):
        df = self.df
        result = df.cols.strip_html(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_strip_html_multiple(self):
        df = self.df
        result = df.cols.strip_html(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_strip_html_NoneType(self):
        df = self.df
        result = df.cols.strip_html(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_strip_html_numeric_float(self):
        df = self.df
        result = df.cols.strip_html(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_strip_html_numeric_int(self):
        df = self.df
        result = df.cols.strip_html(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_strip_html_string(self):
        df = self.df
        result = df.cols.strip_html(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sub_domain_all(self):
        df = self.df
        result = df.cols.sub_domain(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sub_domain_bool(self):
        df = self.df
        result = df.cols.sub_domain(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sub_domain_bytearray(self):
        df = self.df
        result = df.cols.sub_domain(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sub_domain_datetime(self):
        df = self.df
        result = df.cols.sub_domain(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sub_domain_list(self):
        df = self.df
        result = df.cols.sub_domain(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sub_domain_multiple(self):
        df = self.df
        result = df.cols.sub_domain(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_sub_domain_NoneType(self):
        df = self.df
        result = df.cols.sub_domain(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sub_domain_numeric_float(self):
        df = self.df
        result = df.cols.sub_domain(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sub_domain_numeric_int(self):
        df = self.df
        result = df.cols.sub_domain(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_sub_domain_string(self):
        df = self.df
        result = df.cols.sub_domain(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [None, None, None, None, None, None],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tanh_all(self):
        df = self.df
        result = df.cols.tanh(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tanh_bool(self):
        df = self.df
        result = df.cols.tanh(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [0.7615941559557649, 0.7615941559557649, 0.7615941559557649, 0.7615941559557649, 0.7615941559557649, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tanh_bytearray(self):
        df = self.df
        result = df.cols.tanh(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tanh_datetime(self):
        df = self.df
        result = df.cols.tanh(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tanh_list(self):
        df = self.df
        result = df.cols.tanh(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tanh_multiple(self):
        df = self.df
        result = df.cols.tanh(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_tanh_NoneType(self):
        df = self.df
        result = df.cols.tanh(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tanh_numeric_float(self):
        df = self.df
        result = df.cols.tanh(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-1.0, 0.9999999999999966, 1.0, 0.9999999999897818, nan, 1.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tanh_numeric_int(self):
        df = self.df
        result = df.cols.tanh(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [0.9999999958776927, 0.9999983369439447, 0.9999983369439447, 0.9999997749296758, 0.9999999958776927, 0.9999997749296758],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tanh_string(self):
        df = self.df
        result = df.cols.tanh(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tan_all(self):
        df = self.df
        result = df.cols.tan(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tan_bool(self):
        df = self.df
        result = df.cols.tan(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [1.5574077246549023, 1.5574077246549023, 1.5574077246549023, 1.5574077246549023, 1.5574077246549023, 0.0],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tan_bytearray(self):
        df = self.df
        result = df.cols.tan(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tan_datetime(self):
        df = self.df
        result = df.cols.tan(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tan_list(self):
        df = self.df
        result = df.cols.tan(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tan_multiple(self):
        df = self.df
        result = df.cols.tan(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_tan_NoneType(self):
        df = self.df
        result = df.cols.tan(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tan_numeric_float(self):
        df = self.df
        result = df.cols.tan(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [0.28142960456426525, 3.49391564547484, 1.1787535542062797, 0.4630211329364896, nan, 45.244742070819356],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tan_numeric_int(self):
        df = self.df
        result = df.cols.tan(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [0.6483608274590866, 0.8714479827243188, 0.8714479827243188, -6.799711455220379, 0.6483608274590866, -6.799711455220379],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_tan_string(self):
        df = self.df
        result = df.cols.tan(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_title_all(self):
        df = self.df
        result = df.cols.title(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["Bytearray(B'Leader')", "Bytearray(B'Espionage')", "Bytearray(B'Security')", "Bytearray(B'First Lieutenant')", "Bytearray(B'None')", "Bytearray(B'Battle Station')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0'],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'Bumbl#Ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_title_bool(self):
        df = self.df
        result = df.cols.title(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_title_bytearray(self):
        df = self.df
        result = df.cols.title(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["Bytearray(B'Leader')", "Bytearray(B'Espionage')", "Bytearray(B'Security')", "Bytearray(B'First Lieutenant')", "Bytearray(B'None')", "Bytearray(B'Battle Station')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_title_datetime(self):
        df = self.df
        result = df.cols.title(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_title_list(self):
        df = self.df
        result = df.cols.title(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_title_multiple(self):
        df = self.df
        result = df.cols.title(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'Nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_title_NoneType(self):
        df = self.df
        result = df.cols.title(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_title_numeric_float(self):
        df = self.df
        result = df.cols.title(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'Nan', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_title_numeric_int(self):
        df = self.df
        result = df.cols.title(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_title_string(self):
        df = self.df
        result = df.cols.title(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'Bumbl#Ebéé  ', 'Ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_top_domain_all(self):
        df = self.df
        result = df.cols.top_domain(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_top_domain_bool(self):
        df = self.df
        result = df.cols.top_domain(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_top_domain_bytearray(self):
        df = self.df
        result = df.cols.top_domain(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_top_domain_datetime(self):
        df = self.df
        result = df.cols.top_domain(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_top_domain_list(self):
        df = self.df
        result = df.cols.top_domain(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_top_domain_multiple(self):
        df = self.df
        result = df.cols.top_domain(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_top_domain_NoneType(self):
        df = self.df
        result = df.cols.top_domain(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_top_domain_numeric_float(self):
        df = self.df
        result = df.cols.top_domain(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_top_domain_numeric_int(self):
        df = self.df
        result = df.cols.top_domain(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_top_domain_string(self):
        df = self.df
        result = df.cols.top_domain(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [None, None, None, None, None, None],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_to_string_all(self):
        df = self.df
        result = df.cols.to_string(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_to_string_bool(self):
        df = self.df
        result = df.cols.to_string(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_to_string_bytearray(self):
        df = self.df
        result = df.cols.to_string(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_to_string_datetime(self):
        df = self.df
        result = df.cols.to_string(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_to_string_list(self):
        df = self.df
        result = df.cols.to_string(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_to_string_multiple(self):
        df = self.df
        result = df.cols.to_string(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_to_string_NoneType(self):
        df = self.df
        result = df.cols.to_string(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_to_string_numeric_float(self):
        df = self.df
        result = df.cols.to_string(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_to_string_numeric_int(self):
        df = self.df
        result = df.cols.to_string(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_to_string_string(self):
        df = self.df
        result = df.cols.to_string(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_trim_all(self):
        df = self.df
        result = df.cols.trim(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_trim_bool(self):
        df = self.df
        result = df.cols.trim(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['True', 'True', 'True', 'True', 'True', 'False'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_trim_bytearray(self):
        df = self.df
        result = df.cols.trim(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["bytearray(b'Leader')", "bytearray(b'Espionage')", "bytearray(b'Security')", "bytearray(b'First Lieutenant')", "bytearray(b'None')", "bytearray(b'Battle Station')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_trim_datetime(self):
        df = self.df
        result = df.cols.trim(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_trim_list(self):
        df = self.df
        result = df.cols.trim(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[None, 5700.0]', '[91.44, None]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_trim_multiple(self):
        df = self.df
        result = df.cols.trim(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['Inochi', 'Convoy']", "['Bumble', 'Goldback']", "['Roadbuster']", "['Meister']", "['Megatron']", "['Metroflex']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'None', 'None'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_trim_NoneType(self):
        df = self.df
        result = df.cols.trim(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['None', 'None', 'None', 'None', 'None', 'None'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_trim_numeric_float(self):
        df = self.df
        result = df.cols.trim(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_trim_numeric_int(self):
        df = self.df
        result = df.cols.trim(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_trim_string(self):
        df = self.df
        result = df.cols.trim(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_upper_all(self):
        df = self.df
        result = df.cols.upper(cols='*')
        result = result.to_dict()
        expected = {'NullType': ['NONE', 'NONE', 'NONE', 'NONE', 'NONE', 'NONE'],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[NONE, 5700.0]', '[91.44, NONE]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["BYTEARRAY(B'LEADER')", "BYTEARRAY(B'ESPIONAGE')", "BYTEARRAY(B'SECURITY')", "BYTEARRAY(B'FIRST LIEUTENANT')", "BYTEARRAY(B'NONE')", "BYTEARRAY(B'BATTLE STATION')"],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'NAN', '300.0'],
 'japanese name': ["['INOCHI', 'CONVOY']", "['BUMBLE', 'GOLDBACK']", "['ROADBUSTER']", "['MEISTER']", "['MEGATRON']", "['METROFLEX']"],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'NONE', 'NONE'],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': ['TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'FALSE'],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['LEADER', 'ESPIONAGE', 'SECURITY', 'FIRST LIEUTENANT', 'NONE', 'BATTLE STATION'],
 'names': ['OPTIMUS', 'BUMBL#EBÉÉ  ', 'IRONHIDE&', 'JAZZ', 'MEGATRON', 'METROPLEX_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'NAN']}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_upper_bool(self):
        df = self.df
        result = df.cols.upper(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': ['TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'FALSE'],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_upper_bytearray(self):
        df = self.df
        result = df.cols.upper(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': ["BYTEARRAY(B'LEADER')", "BYTEARRAY(B'ESPIONAGE')", "BYTEARRAY(B'SECURITY')", "BYTEARRAY(B'FIRST LIEUTENANT')", "BYTEARRAY(B'NONE')", "BYTEARRAY(B'BATTLE STATION')"],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_upper_datetime(self):
        df = self.df
        result = df.cols.upper(cols=['last date seen'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': ['2016-09-10', '2015-08-10', '2014-07-10', '2013-06-10', '2012-05-10', '2011-04-10'],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_upper_list(self):
        df = self.df
        result = df.cols.upper(cols=['attributes'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': ['[8.5344, 4300.0]', '[5.334, 2000.0]', '[7.9248, 4000.0]', '[3.9624, 1800.0]', '[NONE, 5700.0]', '[91.44, NONE]'],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_upper_multiple(self):
        df = self.df
        result = df.cols.upper(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        expected = {'NullType': ['NONE', 'NONE', 'NONE', 'NONE', 'NONE', 'NONE'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': ["['INOCHI', 'CONVOY']", "['BUMBLE', 'GOLDBACK']", "['ROADBUSTER']", "['MEISTER']", "['MEGATRON']", "['METROFLEX']"],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', 'NONE', 'NONE'],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'],
 'age': ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'],
 'function': ['LEADER', 'ESPIONAGE', 'SECURITY', 'FIRST LIEUTENANT', 'NONE', 'BATTLE STATION'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'],
 'weight(t)': ['4.3', '2.0', '4.0', '1.8', '5.7', 'NAN']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_upper_NoneType(self):
        df = self.df
        result = df.cols.upper(cols=['NullType'])
        result = result.to_dict()
        expected = {'NullType': ['NONE', 'NONE', 'NONE', 'NONE', 'NONE', 'NONE'],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_upper_numeric_float(self):
        df = self.df
        result = df.cols.upper(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': ['-28.0', '17.0', '26.0', '13.0', 'NAN', '300.0'],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_upper_numeric_int(self):
        df = self.df
        result = df.cols.upper(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': ['10', '7', '7', '8', '10', '8'],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_upper_string(self):
        df = self.df
        result = df.cols.upper(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['OPTIMUS', 'BUMBL#EBÉÉ  ', 'IRONHIDE&', 'JAZZ', 'MEGATRON', 'METROPLEX_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_file_all(self):
        df = self.df
        result = df.cols.url_file(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_file_bool(self):
        df = self.df
        result = df.cols.url_file(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_file_bytearray(self):
        df = self.df
        result = df.cols.url_file(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_file_datetime(self):
        df = self.df
        result = df.cols.url_file(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_file_list(self):
        df = self.df
        result = df.cols.url_file(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_file_multiple(self):
        df = self.df
        result = df.cols.url_file(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_url_file_NoneType(self):
        df = self.df
        result = df.cols.url_file(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_file_numeric_float(self):
        df = self.df
        result = df.cols.url_file(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_file_numeric_int(self):
        df = self.df
        result = df.cols.url_file(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_file_string(self):
        df = self.df
        result = df.cols.url_file(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [None, None, '&', None, None, '_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_fragment_all(self):
        df = self.df
        result = df.cols.url_fragment(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_fragment_bool(self):
        df = self.df
        result = df.cols.url_fragment(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_fragment_bytearray(self):
        df = self.df
        result = df.cols.url_fragment(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_fragment_datetime(self):
        df = self.df
        result = df.cols.url_fragment(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_fragment_list(self):
        df = self.df
        result = df.cols.url_fragment(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_fragment_multiple(self):
        df = self.df
        result = df.cols.url_fragment(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_url_fragment_NoneType(self):
        df = self.df
        result = df.cols.url_fragment(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_fragment_numeric_float(self):
        df = self.df
        result = df.cols.url_fragment(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_fragment_numeric_int(self):
        df = self.df
        result = df.cols.url_fragment(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_fragment_string(self):
        df = self.df
        result = df.cols.url_fragment(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [None, 'ebéé  ', None, None, None, None],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_path_all(self):
        df = self.df
        result = df.cols.url_path(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_path_bool(self):
        df = self.df
        result = df.cols.url_path(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_path_bytearray(self):
        df = self.df
        result = df.cols.url_path(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_path_datetime(self):
        df = self.df
        result = df.cols.url_path(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_path_list(self):
        df = self.df
        result = df.cols.url_path(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_path_multiple(self):
        df = self.df
        result = df.cols.url_path(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_url_path_NoneType(self):
        df = self.df
        result = df.cols.url_path(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_path_numeric_float(self):
        df = self.df
        result = df.cols.url_path(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_path_numeric_int(self):
        df = self.df
        result = df.cols.url_path(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_path_string(self):
        df = self.df
        result = df.cols.url_path(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [None, None, '&', None, None, '_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_query_all(self):
        df = self.df
        result = df.cols.url_query(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_query_bool(self):
        df = self.df
        result = df.cols.url_query(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_query_bytearray(self):
        df = self.df
        result = df.cols.url_query(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_query_datetime(self):
        df = self.df
        result = df.cols.url_query(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_query_list(self):
        df = self.df
        result = df.cols.url_query(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_query_multiple(self):
        df = self.df
        result = df.cols.url_query(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_url_query_NoneType(self):
        df = self.df
        result = df.cols.url_query(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_query_numeric_float(self):
        df = self.df
        result = df.cols.url_query(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_query_numeric_int(self):
        df = self.df
        result = df.cols.url_query(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_query_string(self):
        df = self.df
        result = df.cols.url_query(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [None, None, None, None, None, None],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_scheme_all(self):
        df = self.df
        result = df.cols.url_scheme(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_scheme_bool(self):
        df = self.df
        result = df.cols.url_scheme(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_scheme_bytearray(self):
        df = self.df
        result = df.cols.url_scheme(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_scheme_datetime(self):
        df = self.df
        result = df.cols.url_scheme(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_scheme_list(self):
        df = self.df
        result = df.cols.url_scheme(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_scheme_multiple(self):
        df = self.df
        result = df.cols.url_scheme(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_url_scheme_NoneType(self):
        df = self.df
        result = df.cols.url_scheme(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_scheme_numeric_float(self):
        df = self.df
        result = df.cols.url_scheme(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_scheme_numeric_int(self):
        df = self.df
        result = df.cols.url_scheme(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_url_scheme_string(self):
        df = self.df
        result = df.cols.url_scheme(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [None, None, None, None, None, None],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_count_all(self):
        df = self.df
        result = df.cols.word_count(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_count_bool(self):
        df = self.df
        result = df.cols.word_count(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_count_bytearray(self):
        df = self.df
        result = df.cols.word_count(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_count_datetime(self):
        df = self.df
        result = df.cols.word_count(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_count_list(self):
        df = self.df
        result = df.cols.word_count(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_count_multiple(self):
        df = self.df
        result = df.cols.word_count(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_word_count_NoneType(self):
        df = self.df
        result = df.cols.word_count(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_count_numeric_float(self):
        df = self.df
        result = df.cols.word_count(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_count_numeric_int(self):
        df = self.df
        result = df.cols.word_count(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_count_string(self):
        df = self.df
        result = df.cols.word_count(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [11, 22, 17, 8, 12, 29],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_tokenizer_all(self):
        df = self.df
        result = df.cols.word_tokenizer(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_tokenizer_bool(self):
        df = self.df
        result = df.cols.word_tokenizer(cols=['Cybertronian'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_tokenizer_bytearray(self):
        df = self.df
        result = df.cols.word_tokenizer(cols=['function(binary)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_tokenizer_datetime(self):
        df = self.df
        result = df.cols.word_tokenizer(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_tokenizer_list(self):
        df = self.df
        result = df.cols.word_tokenizer(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_tokenizer_multiple(self):
        df = self.df
        result = df.cols.word_tokenizer(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_word_tokenizer_NoneType(self):
        df = self.df
        result = df.cols.word_tokenizer(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_tokenizer_numeric_float(self):
        df = self.df
        result = df.cols.word_tokenizer(cols=['height(ft)'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_tokenizer_numeric_int(self):
        df = self.df
        result = df.cols.word_tokenizer(cols=['rank'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_word_tokenizer_string(self):
        df = self.df
        result = df.cols.word_tokenizer(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [['Optimus'], ['bumbl', '#', 'ebéé'], ['ironhide', '&'], ['Jazz'], ['Megatron'], ['Metroplex_', ')', '^', '$']],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_z_score_all(self):
        df = self.df
        result = df.cols.z_score(cols='*')
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_z_score_bool(self):
        df = self.df
        result = df.cols.z_score(cols=['Cybertronian'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [-1.2360679774997898, -1.2360679774997898, -1.2360679774997898, -1.2360679774997898, -1.2360679774997898, -2.23606797749979],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_z_score_bytearray(self):
        df = self.df
        result = df.cols.z_score(cols=['function(binary)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [nan, nan, nan, nan, nan, nan],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_z_score_datetime(self):
        df = self.df
        result = df.cols.z_score(cols=['last date seen'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_z_score_list(self):
        df = self.df
        result = df.cols.z_score(cols=['attributes'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_z_score_multiple(self):
        df = self.df
        result = df.cols.z_score(cols=['NullType', 'weight(t)', 'japanese name', 'timestamp', 'function', 'age',
 'Date Type', 'last position seen', 'date arrival'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_z_score_NoneType(self):
        df = self.df
        result = df.cols.z_score(cols=['NullType'])
        result = result.to_dict()
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_z_score_numeric_float(self):
        df = self.df
        result = df.cols.z_score(cols=['height(ft)'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.552839175542417, 16.447160824457583, 25.447160824457583, 12.447160824457582, nan, 299.44716082445757],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_z_score_numeric_int(self):
        df = self.df
        result = df.cols.z_score(cols=['rank'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [3.3184689521893898, 0.31846895218938975, 0.31846895218938975, 1.3184689521893898, 3.3184689521893898, 1.3184689521893898],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))
    
    def test_cols_z_score_string(self):
        df = self.df
        result = df.cols.z_score(cols=['names'])
        result = result.to_dict()
        expected = {'NullType': [None, None, None, None, None, None],
 'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
 'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
 'function(binary)': [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')],
 'height(ft)': [-28.0, 17.0, 26.0, 13.0, nan, 300.0],
 'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
 'last date seen': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-07-10 00:00:00'), Timestamp('2013-06-10 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
 'rank': [10, 7, 7, 8, 10, 8],
 'Cybertronian': [True, True, True, True, True, False],
 'Date Type': [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')],
 'age': [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
 'function': ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
 'names': [nan, nan, nan, nan, nan, nan],
 'timestamp': [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')],
 'weight(t)': [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))

class TestColsDask(TestColsPandas):
    config = {'engine': 'dask', 'n_partitions': 1}

class TestColsDask2(TestColsPandas):
    config = {'engine': 'dask', 'n_partitions': 2}
