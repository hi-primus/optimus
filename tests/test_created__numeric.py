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


class TestNumericPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None

    def test_cols_abs(self):
        df = self.create_dataframe(data={('abs_test', 'object'): [-1, '10', -inf, nan, 0, None]}, force_data_types=True)
        result = df.cols.abs(cols=['abs_test'])
        expected = self.create_dataframe(data={('abs_test', 'float64'): [1.0, 10.0, inf, nan, 0.0, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_abs_all(self):
        df = self.df.copy()
        result = df.cols.abs(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [10.0, 7.0, 7.0, 8.0, 10.0, 8.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1.4734656e+18, 1.4391648e+18, 1.403568e+18, 1.372032e+18, 1.336608e+18, 1.3023936e+18], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_abs_multiple(self):
        df = self.df.copy()
        result = df.cols.abs(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_abs_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.abs(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [28.0, 17.0, 26.0, 13.0, nan, 300.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_abs_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.abs(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ceil(self):
        df = self.create_dataframe(data={('ceil_test', 'object'): [inf, '12.342', 0, None, 1004.5, -27.7]}, force_data_types=True)
        result = df.cols.ceil(cols=['ceil_test'])
        expected = self.create_dataframe(data={('ceil_test', 'float64'): [inf, 13.0, 0.0, nan, 1005.0, -27.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ceil_all(self):
        df = self.df.copy()
        result = df.cols.ceil(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [10.0, 7.0, 7.0, 8.0, 10.0, 8.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1.4734656e+18, 1.4391648e+18, 1.403568e+18, 1.372032e+18, 1.336608e+18, 1.3023936e+18], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18], ('weight(t)', 'float64'): [5.0, 2.0, 4.0, 2.0, 6.0, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ceil_multiple(self):
        df = self.df.copy()
        result = df.cols.ceil(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [5.0, 2.0, 4.0, 2.0, 6.0, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ceil_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.ceil(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ceil_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.ceil(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_exp(self):
        df = self.create_dataframe(data={('exp_test', 'object'): [0, '0.5', -0.5, 2.718, inf, None]}, force_data_types=True)
        result = df.cols.exp(cols=['exp_test'])
        expected = self.create_dataframe(data={('exp_test', 'float64'): [1.0, 1.6487212707001282, 0.6065306597126334, 15.149991940878165, inf, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_exp_all(self):
        df = self.df.copy()
        result = df.cols.exp(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [6.914400106940203e-13, 24154952.7535753, 195729609428.83878, 442413.3920089205, nan, 1.9424263952412558e+130], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [22026.465794806718, 1096.6331584284585, 1096.6331584284585, 2980.9579870417283, 22026.465794806718, 2980.9579870417283], ('Cybertronian', 'float64'): [2.718281828459045, 2.718281828459045, 2.718281828459045, 2.718281828459045, 2.718281828459045, 1.0], ('Date Type', 'float64'): [inf, inf, inf, inf, inf, inf], ('age', 'float64'): [inf, inf, inf, inf, inf, inf], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [inf, inf, inf, inf, inf, inf], ('weight(t)', 'float64'): [73.69979369959579, 7.38905609893065, 54.598150033144236, 6.0496474644129465, 298.8674009670603, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_exp_multiple(self):
        df = self.df.copy()
        result = df.cols.exp(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [73.69979369959579, 7.38905609893065, 54.598150033144236, 6.0496474644129465, 298.8674009670603, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_exp_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.exp(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [6.914400106940203e-13, 24154952.7535753, 195729609428.83878, 442413.3920089205, nan, 1.9424263952412558e+130]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_exp_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.exp(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_floor(self):
        df = self.create_dataframe(data={('floor_test', 'object'): [inf, '12.342', 0, None, 1004.5, -27.7]}, force_data_types=True)
        result = df.cols.floor(cols=['floor_test'])
        expected = self.create_dataframe(data={('floor_test', 'float64'): [inf, 12.0, 0.0, nan, 1004.0, -28.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_floor_all(self):
        df = self.df.copy()
        result = df.cols.floor(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [10.0, 7.0, 7.0, 8.0, 10.0, 8.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1.4734656e+18, 1.4391648e+18, 1.403568e+18, 1.372032e+18, 1.336608e+18, 1.3023936e+18], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18], ('weight(t)', 'float64'): [4.0, 2.0, 4.0, 1.0, 5.0, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_floor_multiple(self):
        df = self.df.copy()
        result = df.cols.floor(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [4.0, 2.0, 4.0, 1.0, 5.0, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_floor_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.floor(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_floor_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.floor(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ln(self):
        df = self.create_dataframe(data={('ln_test', 'object'): ['0.36', 1, inf, 0, 2.7182, -100]}, force_data_types=True)
        result = df.cols.ln(cols=['ln_test'])
        expected = self.create_dataframe(data={('ln_test', 'float64'): [-1.0216512475319814, 0.0, inf, -inf, 0.9999698965391098, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ln_all(self):
        df = self.df.copy()
        result = df.cols.ln(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [nan, 2.833213344056216, 3.258096538021482, 2.5649493574615367, nan, 5.703782474656201], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [2.302585092994046, 1.9459101490553132, 1.9459101490553132, 2.0794415416798357, 2.302585092994046, 2.0794415416798357], ('Cybertronian', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, -inf], ('Date Type', 'float64'): [41.83414885104055, 41.810594619215664, 41.78554923984176, 41.76282452653968, 41.73666673530567, 41.71073547614197], ('age', 'float64'): [15.424948470398375, 15.424948470398375, 15.424948470398375, 15.424948470398375, 15.424948470398375, 15.424948470398375], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [41.78554923984176, 41.78554923984176, 41.78554923984176, 41.78554923984176, 41.78554923984176, 41.78554923984176], ('weight(t)', 'float64'): [1.4586150226995167, 0.6931471805599453, 1.3862943611198906, 0.5877866649021191, 1.7404661748405046, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ln_multiple(self):
        df = self.df.copy()
        result = df.cols.ln(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [1.4586150226995167, 0.6931471805599453, 1.3862943611198906, 0.5877866649021191, 1.7404661748405046, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ln_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.ln(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, 2.833213344056216, 3.258096538021482, 2.5649493574615367, nan, 5.703782474656201]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_ln_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.ln(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_log(self):
        df = self.create_dataframe(data={('log_test', 'float64'): [10.0, nan, inf, -356.0, 0.5314, 0.0]}, force_data_types=True)
        result = df.cols.log(cols=['log_test'], base=10)
        expected = self.create_dataframe(data={('log_test', 'float64'): [1.0, nan, inf, nan, -0.2745784499257413, -inf]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_log_1(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.log(cols=['height(ft)'], base=100.3)
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, 0.6148245379175669, 0.7070267767453151, 0.5566096202361945, nan, 1.2377555088534953]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_log_2(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.log(cols=['height(ft)'], base=2.7182)
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, 2.8332986361508996, 3.258194620955827, 2.5650265736386784, nan, 5.70395418341788]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_log_3(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.log(cols=['height(ft)'], base=-3)
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_log_all(self):
        df = self.df.copy()
        result = df.cols.log(cols='*', base=12)
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [nan, 1.1401689251779061, 1.3111545008338428, 1.032211555182713, nan, 2.2953709247559937], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [0.9266284080291269, 0.783091851446946, 0.783091851446946, 0.8368288369533894, 0.9266284080291269, 0.8368288369533894], ('Cybertronian', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, -inf], ('Date Type', 'float64'): [16.835299971775452, 16.825821051580643, 16.81574204946761, 16.806596952083762, 16.796070282505955, 16.785634776139585], ('age', 'float64'): [6.207455910552758, 6.207455910552758, 6.207455910552758, 6.207455910552758, 6.207455910552758, 6.207455910552758], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [16.81574204946761, 16.81574204946761, 16.81574204946761, 16.81574204946761, 16.81574204946761, 16.81574204946761], ('weight(t)', 'float64'): [0.5869898665303819, 0.2789429456511298, 0.5578858913022596, 0.23654275501748367, 0.7004151141810467, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_log_multiple(self):
        df = self.df.copy()
        result = df.cols.log(cols=['NullType', 'weight(t)', 'japanese name'], base=21, output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [0.4790948506821362, 0.227670248696953, 0.455340497393906, 0.1930636666096123, 0.5716713246304594, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_log_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.log(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, 1.2304489213782739, 1.414973347970818, 1.1139433523068367, nan, 2.477121254719662]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_log_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.log(cols=['names'], base=2, output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_max_abs_scaler_all(self):
        df = self.df.copy()
        result = df.cols.max_abs_scaler(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-0.09333333333333334, 0.056666666666666664, 0.08666666666666667, 0.043333333333333335, nan, 1.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [1.0, 0.7, 0.7, 0.8, 1.0, 0.8], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1.0, 0.9767210038700598, 0.9525624486923889, 0.9311598451976076, 0.9071185645596341, 0.8838982056995426], ('age', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 1.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 1.0], ('weight(t)', 'float64'): [0.7543859649122806, 0.3508771929824561, 0.7017543859649122, 0.3157894736842105, 1.0, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_max_abs_scaler_multiple(self):
        df = self.df.copy()
        result = df.cols.max_abs_scaler(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [0.7543859649122806, 0.3508771929824561, 0.7017543859649122, 0.3157894736842105, 1.0, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_max_abs_scaler_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.max_abs_scaler(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-0.09333333333333334, 0.056666666666666664, 0.08666666666666667, 0.043333333333333335, nan, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_max_abs_scaler_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.max_abs_scaler(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_min_max_scaler_all(self):
        df = self.df.copy()
        result = df.cols.min_max_scaler(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [0.0, 0.13719512195121952, 0.16463414634146342, 0.125, nan, 1.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [1.0, 0.0, 0.0, 0.3333333333333335, 1.0, 0.3333333333333335], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1.0000000000000009, 0.7994949494949504, 0.5914141414141421, 0.4070707070707078, 0.20000000000000018, 0.0], ('age', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, 0.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, 0.0], ('weight(t)', 'float64'): [0.641025641025641, 0.051282051282051266, 0.5641025641025641, 0.0, 1.0, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_min_max_scaler_multiple(self):
        df = self.df.copy()
        result = df.cols.min_max_scaler(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [0.641025641025641, 0.051282051282051266, 0.5641025641025641, 0.0, 1.0, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_min_max_scaler_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.min_max_scaler(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [0.0, 0.13719512195121952, 0.16463414634146342, 0.125, nan, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_min_max_scaler_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.min_max_scaler(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_mod(self):
        df = self.create_dataframe(data={('mod_test', 'float64'): [10.0, nan, inf, -356.0, 0.5314, 0.0]}, force_data_types=True)
        result = df.cols.mod(cols=['mod_test'], divisor=3)
        expected = self.create_dataframe(data={('mod_test', 'float64'): [1.0, nan, nan, 1.0, 0.5314, 0.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_mod_1(self):
        df = self.create_dataframe(data={('mod_test', 'float64'): [10.0, nan, inf, -356.0, 0.5314, 0.0]}, force_data_types=True)
        result = df.cols.mod(cols=['mod_test'], divisor=100.3)
        expected = self.create_dataframe(data={('mod_test', 'float64'): [10.0, nan, nan, 45.19999999999999, 0.5314, 0.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_mod_2(self):
        df = self.create_dataframe(data={('mod_test', 'float64'): [10.0, nan, inf, -356.0, 0.5314, 0.0]}, force_data_types=True)
        result = df.cols.mod(cols=['mod_test'], divisor=6)
        expected = self.create_dataframe(data={('mod_test', 'float64'): [4.0, nan, nan, 4.0, 0.5314, 0.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_mod_3(self):
        df = self.create_dataframe(data={('mod_test', 'float64'): [10.0, nan, inf, -356.0, 0.5314, 0.0]}, force_data_types=True)
        result = df.cols.mod(cols=['mod_test'], divisor=-12)
        expected = self.create_dataframe(data={('mod_test', 'float64'): [-2.0, nan, nan, -8.0, -11.4686, -0.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_mod_all(self):
        df = self.df.copy()
        result = df.cols.mod(cols='*', divisor=5)
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [2.0, 2.0, 1.0, 3.0, nan, 0.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [0.0, 2.0, 2.0, 3.0, 0.0, 3.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, 0.0], ('age', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, 0.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, 0.0], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 0.7000000000000002, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_mod_multiple(self):
        df = self.df.copy()
        result = df.cols.mod(cols=['NullType', 'weight(t)', 'japanese name'], divisor=10, output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_mod_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.mod(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [0.0, 1.0, 0.0, 1.0, nan, 0.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_mod_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.mod(cols=['names'], divisor=4, output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_modified_z_score_all(self):
        df = self.df.copy()
        result = df.cols.modified_z_score(cols='*', estimate=False)
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [3.3725, 0.0, 0.6745, 0.29977777777777775, nan, 21.20927777777778], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [1.349, 0.6745, 0.6745, 0.0, 1.349, 0.0], ('Cybertronian', 'float64'): [nan, nan, nan, nan, nan, inf], ('Date Type', 'float64'): [1.1268184498736311, 0.6756364785172704, 0.2074073294018534, 0.2074073294018534, 0.6733635214827296, 1.1234090143218196], ('age', 'float64'): [nan, nan, nan, nan, nan, nan], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [nan, nan, nan, nan, nan, nan], ('weight(t)', 'float64'): [0.11902941176470579, 0.7935294117647058, 0.0, 0.8728823529411763, 0.6745, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_modified_z_score_multiple(self):
        df = self.df.copy()
        result = df.cols.modified_z_score(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'], estimate=False)
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [0.11902941176470579, 0.7935294117647058, 0.0, 0.8728823529411763, 0.6745, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_modified_z_score_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.modified_z_score(cols=['height(ft)'], estimate=False)
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [3.3725, 0.0, 0.6745, 0.29977777777777775, nan, 21.20927777777778]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_modified_z_score_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.modified_z_score(cols=['names'], output_cols=['names_2'], estimate=False)
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pow(self):
        df = self.create_dataframe(data={('pow_test', 'float64'): [10.0, nan, -inf, -356.0, 0.5314, 0.0]}, force_data_types=True)
        result = df.cols.pow(cols=['pow_test'], power=2)
        expected = self.create_dataframe(data={('pow_test', 'float64'): [100.0, nan, inf, 126736.0, 0.28238596, 0.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pow_1(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.pow(cols=['height(ft)'], power=0.5)
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, 4.123105625617661, 5.0990195135927845, 3.605551275463989, nan, 17.320508075688775]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pow_2(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.pow(cols=['height(ft)'], power=10)
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [296196766695424.0, 2015993900449.0, 141167095653376.0, 137858491849.0, nan, 5.9049e+24]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pow_3(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.pow(cols=['height(ft)'], power=-5)
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-5.8104510025584576e-08, 7.042962777237426e-07, 8.416533573215762e-08, 2.693290743429044e-06, nan, 4.1152263374485594e-13]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pow_all(self):
        df = self.df.copy()
        result = df.cols.pow(cols='*', power=3)
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-21952.0, 4913.0, 17576.0, 2197.0, nan, 27000000.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [1000.0, 343.0, 343.0, 512.0, 1000.0, 512.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [3.199042452533802e+54, 2.9807914007124516e+54, 2.7650333540436665e+54, 2.582811561078817e+54, 2.3878781881420677e+54, 2.2091579100654466e+54], ('age', 'float64'): [1.25e+20, 1.25e+20, 1.25e+20, 1.25e+20, 1.25e+20, 1.25e+20], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [2.7650333540436665e+54, 2.7650333540436665e+54, 2.7650333540436665e+54, 2.7650333540436665e+54, 2.7650333540436665e+54, 2.7650333540436665e+54], ('weight(t)', 'float64'): [79.50699999999999, 8.0, 64.0, 5.832000000000001, 185.193, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pow_multiple(self):
        df = self.df.copy()
        result = df.cols.pow(cols=['NullType', 'weight(t)', 'japanese name'], power=117, output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [1.3055974789624274e+74, 1.661534994731145e+35, 2.7606985387162255e+70, 7.360089527435959e+29, 2.73752512412485e+88, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pow_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.pow(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [784.0, 289.0, 676.0, 169.0, nan, 90000.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_pow_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.pow(cols=['names'], power=3.7, output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reciprocal(self):
        df = self.create_dataframe(data={('reciprocal_test', 'object'): [1, 0, -inf, '237', None, 0.125]}, force_data_types=True)
        result = df.cols.reciprocal(cols=['reciprocal_test'])
        expected = self.create_dataframe(data={('reciprocal_test', 'float64'): [1.0, inf, -0.0, 0.004219409282700422, nan, 8.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reciprocal_all(self):
        df = self.df.copy()
        result = df.cols.reciprocal(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-0.03571428571428571, 0.058823529411764705, 0.038461538461538464, 0.07692307692307693, nan, 0.0033333333333333335], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [0.1, 0.14285714285714285, 0.14285714285714285, 0.125, 0.1, 0.125], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, inf], ('Date Type', 'float64'): [6.786721047305075e-19, 6.948474559689064e-19, 7.12469933768795e-19, 7.28845974437914e-19, 7.481625128683952e-19, 7.678170408699797e-19], ('age', 'float64'): [2e-07, 2e-07, 2e-07, 2e-07, 2e-07, 2e-07], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [7.12469933768795e-19, 7.12469933768795e-19, 7.12469933768795e-19, 7.12469933768795e-19, 7.12469933768795e-19, 7.12469933768795e-19], ('weight(t)', 'float64'): [0.23255813953488372, 0.5, 0.25, 0.5555555555555556, 0.17543859649122806, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reciprocal_multiple(self):
        df = self.df.copy()
        result = df.cols.reciprocal(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [0.23255813953488372, 0.5, 0.25, 0.5555555555555556, 0.17543859649122806, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reciprocal_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.reciprocal(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-0.03571428571428571, 0.058823529411764705, 0.038461538461538464, 0.07692307692307693, nan, 0.0033333333333333335]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_reciprocal_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.reciprocal(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_round(self):
        df = self.create_dataframe(data={('round_test', 'float64'): [10.0, nan, -inf, -356.312312, 0.5314, 1.000009]}, force_data_types=True)
        result = df.cols.round(cols=['round_test'], decimals=2)
        expected = self.create_dataframe(data={('round_test', 'float64'): [10.0, nan, -inf, -356.31, 0.53, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_round_1(self):
        df = self.create_dataframe(data={('round_test', 'float64'): [10.0, nan, -inf, -356.312312, 0.5314, 1.000009]}, force_data_types=True)
        result = df.cols.round(cols=['round_test'], decimals=1)
        expected = self.create_dataframe(data={('round_test', 'float64'): [10.0, nan, -inf, -356.3, 0.5, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_round_2(self):
        df = self.create_dataframe(data={('round_test', 'float64'): [10.0, nan, -inf, -356.312312, 0.5314, 1.000009]}, force_data_types=True)
        result = df.cols.round(cols=['round_test'], decimals=2)
        expected = self.create_dataframe(data={('round_test', 'float64'): [10.0, nan, -inf, -356.31, 0.53, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_round_3(self):
        df = self.create_dataframe(data={('round_test', 'float64'): [10.0, nan, -inf, -356.312312, 0.5314, 1.000009]}, force_data_types=True)
        result = df.cols.round(cols=['round_test'], decimals=5)
        expected = self.create_dataframe(data={('round_test', 'float64'): [10.0, nan, -inf, -356.31231, 0.5314, 1.00001]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_round_all(self):
        df = self.df.copy()
        result = df.cols.round(cols='*', decimals=4)
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [10.0, 7.0, 7.0, 8.0, 10.0, 8.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1.4734656e+18, 1.4391648e+18, 1.403568e+18, 1.372032e+18, 1.336608e+18, 1.3023936e+18], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_round_multiple(self):
        df = self.df.copy()
        result = df.cols.round(cols=['NullType', 'weight(t)', 'japanese name'], decimals=21, output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_round_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.round(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_round_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.round(cols=['names'], decimals=5, output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sqrt(self):
        df = self.create_dataframe(data={('sqrt_test', 'object'): ['10000', 0.25, -81, inf, 0, 1]}, force_data_types=True)
        result = df.cols.sqrt(cols=['sqrt_test'])
        expected = self.create_dataframe(data={('sqrt_test', 'float64'): [100.0, 0.5, nan, inf, 0.0, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sqrt_all(self):
        df = self.df.copy()
        result = df.cols.sqrt(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [nan, 4.123105625617661, 5.0990195135927845, 3.605551275463989, nan, 17.320508075688775], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [3.1622776601683795, 2.6457513110645907, 2.6457513110645907, 2.8284271247461903, 3.1622776601683795, 2.8284271247461903], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1213863913.2950613, 1199651949.5253613, 1184722752.3771122, 1171337696.8235931, 1156117641.0729144, 1141224605.4129748], ('age', 'float64'): [2236.06797749979, 2236.06797749979, 2236.06797749979, 2236.06797749979, 2236.06797749979, 2236.06797749979], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1184722752.3771122, 1184722752.3771122, 1184722752.3771122, 1184722752.3771122, 1184722752.3771122, 1184722752.3771122], ('weight(t)', 'float64'): [2.073644135332772, 1.4142135623730951, 2.0, 1.3416407864998738, 2.3874672772626644, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sqrt_multiple(self):
        df = self.df.copy()
        result = df.cols.sqrt(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [2.073644135332772, 1.4142135623730951, 2.0, 1.3416407864998738, 2.3874672772626644, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sqrt_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.sqrt(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, 4.123105625617661, 5.0990195135927845, 3.605551275463989, nan, 17.320508075688775]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sqrt_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.sqrt(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_standard_scaler_all(self):
        df = self.df.copy()
        result = df.cols.standard_scaler(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-0.7888071163227179, -0.4095729257829497, -0.333726087674996, -0.44328263160870685, nan, 1.9753887613893708], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [1.3363062095621216, -1.069044967649698, -1.069044967649698, -0.2672612419124249, 1.3363062095621216, -0.2672612419124249], ('Cybertronian', 'float64'): [0.4472135954999578, 0.4472135954999578, 0.4472135954999578, 0.4472135954999578, 0.4472135954999578, -2.23606797749979], ('Date Type', 'float64'): [1.4683126020200388, 0.879900654171497, 0.26925651685764507, -0.2717267601315684, -0.8794066055167122, -1.4663364074009], ('age', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, 0.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, 0.0], ('weight(t)', 'float64'): [0.5022984399896845, -1.0588994140323083, 0.2986639372911638, -1.1946557491646554, 1.4525927859161152, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_standard_scaler_multiple(self):
        df = self.df.copy()
        result = df.cols.standard_scaler(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [0.5022984399896845, -1.0588994140323083, 0.2986639372911638, -1.1946557491646554, 1.4525927859161152, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_standard_scaler_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.standard_scaler(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-0.7888071163227179, -0.4095729257829497, -0.333726087674996, -0.44328263160870685, nan, 1.9753887613893708]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_standard_scaler_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.standard_scaler(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_float(self):
        df = self.create_dataframe(data={('to_float_test', 'object'): [-inf, 10001, 0, None, '-41', 5]}, force_data_types=True)
        result = df.cols.to_float(cols=['to_float_test'])
        expected = self.create_dataframe(data={('to_float_test', 'float64'): [-inf, 10001.0, 0.0, nan, -41.0, 5.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_float_all(self):
        df = self.df.copy()
        result = df.cols.to_float(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [10.0, 7.0, 7.0, 8.0, 10.0, 8.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1.4734656e+18, 1.4391648e+18, 1.403568e+18, 1.372032e+18, 1.336608e+18, 1.3023936e+18], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_float_multiple(self):
        df = self.df.copy()
        result = df.cols.to_float(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_float_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.to_float(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_float_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.to_float(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_integer(self):
        df = self.create_dataframe(data={('to_integer_test', 'object'): [inf, '12.342', 0.32, None, 1004.5, -27.7]}, force_data_types=True)
        result = df.cols.to_integer(cols=['to_integer_test'])
        expected = self.create_dataframe(data={('to_integer_test', 'int32'): [0, 12, 0, 0, 1004, -27]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_integer_all(self):
        df = self.df.copy()
        result = df.cols.to_integer(cols='*')
        expected = self.create_dataframe(data={('NullType', 'int32'): [0, 0, 0, 0, 0, 0], ('attributes', 'int64'): [0, 0, 0, 0, 0, 0], ('date arrival', 'int32'): [0, 0, 0, 0, 0, 0], ('function(binary)', 'int32'): [0, 0, 0, 0, 0, 0], ('height(ft)', 'int32'): [-28, 17, 26, 13, 0, 300], ('japanese name', 'int64'): [0, 0, 0, 0, 0, 0], ('last date seen', 'int32'): [0, 0, 0, 0, 0, 0], ('last position seen', 'int32'): [0, 0, 0, 0, 0, 0], ('rank', 'int32'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'int32'): [1, 1, 1, 1, 1, 0], ('Date Type', 'int64'): [1473465600000000000, 1439164800000000000, 1403568000000000000, 1372032000000000000, 1336608000000000000, 1302393600000000000], ('age', 'int32'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'int32'): [0, 0, 0, 0, 0, 0], ('names', 'int32'): [0, 0, 0, 0, 0, 0], ('timestamp', 'int64'): [1403568000000000000, 1403568000000000000, 1403568000000000000, 1403568000000000000, 1403568000000000000, 1403568000000000000], ('weight(t)', 'int32'): [4, 2, 4, 1, 5, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_integer_multiple(self):
        df = self.df.copy()
        result = df.cols.to_integer(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'int32'): [0, 0, 0, 0, 0, 0], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'int64'): [0, 0, 0, 0, 0, 0], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'int32'): [4, 2, 4, 1, 5, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_integer_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.to_integer(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'int32'): [-28, 17, 26, 13, 0, 300]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_to_integer_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.to_integer(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'int32'): [0, 0, 0, 0, 0, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_z_score_all(self):
        df = self.df.copy()
        result = df.cols.z_score(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-28.552839175542417, 16.447160824457583, 25.447160824457583, 12.447160824457582, nan, 299.44716082445757], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [3.3184689521893898, 0.31846895218938975, 0.31846895218938975, 1.3184689521893898, 3.3184689521893898, 1.3184689521893898], ('Cybertronian', 'float64'): [-1.2360679774997898, -1.2360679774997898, -1.2360679774997898, -1.2360679774997898, -1.2360679774997898, -2.23606797749979], ('Date Type', 'float64'): [1.4734656e+18, 1.4391648e+18, 1.403568e+18, 1.372032e+18, 1.336608e+18, 1.3023936e+18], ('age', 'float64'): [-inf, -inf, -inf, -inf, -inf, -inf], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [-inf, -inf, -inf, -inf, -inf, -inf], ('weight(t)', 'float64'): [1.8835372346442196, -0.4164627653557802, 1.5835372346442198, -0.6164627653557802, 3.28353723464422, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_z_score_multiple(self):
        df = self.df.copy()
        result = df.cols.z_score(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [1.8835372346442196, -0.4164627653557802, 1.5835372346442198, -0.6164627653557802, 3.28353723464422, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_z_score_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.z_score(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-28.552839175542417, 16.447160824457583, 25.447160824457583, 12.447160824457582, nan, 299.44716082445757]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_z_score_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.z_score(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))


class TestNumericDask(TestNumericPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestNumericPartitionDask(TestNumericPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestNumericCUDF(TestNumericPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestNumericDC(TestNumericPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestNumericPartitionDC(TestNumericPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestNumericSpark(TestNumericPandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestNumericVaex(TestNumericPandas):
        config = {'engine': 'vaex'}
