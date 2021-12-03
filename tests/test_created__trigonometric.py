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


class TestTrigonometricPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None

    def test_cols_acos(self):
        df = self.create_dataframe(data={('acos_test', 'object'): [1, '0', 9, nan, -inf, None]}, force_data_types=True)
        result = df.cols.acos(cols=['acos_test'])
        expected = self.create_dataframe(data={('acos_test', 'float64'): [0.0, 1.5707963267948966, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_acos_all(self):
        df = self.df.copy()
        result = df.cols.acos(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [nan, nan, nan, nan, nan, nan], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [nan, nan, nan, nan, nan, nan], ('Cybertronian', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, 1.5707963267948966], ('Date Type', 'float64'): [nan, nan, nan, nan, nan, nan], ('age', 'float64'): [nan, nan, nan, nan, nan, nan], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [nan, nan, nan, nan, nan, nan], ('weight(t)', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_acos_multiple(self):
        df = self.df.copy()
        result = df.cols.acos(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_acos_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.acos(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_acos_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.acos(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_acosh(self):
        df = self.create_dataframe(data={('acosh_test', 'float64'): [nan, nan, 1.0, 0.0, -inf, 813.0]}, force_data_types=True)
        result = df.cols.acosh(cols=['acosh_test'])
        expected = self.create_dataframe(data={('acosh_test', 'float64'): [nan, nan, 0.0, nan, nan, 7.393877911874976]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_acosh_all(self):
        df = self.df.copy()
        result = df.cols.acosh(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [nan, 3.5254943480781717, 3.95087369077445, 3.2566139548000526, nan, 6.396926877426794], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [2.993222846126381, 2.6339157938496336, 2.6339157938496336, 2.7686593833135738, 2.993222846126381, 2.7686593833135738], ('Cybertronian', 'float64'): [0.0, 0.0, 0.0, 0.0, 0.0, nan], ('Date Type', 'float64'): [42.5272960316005, 42.50374179977561, 42.478696420401704, 42.45597170709963, 42.429813915865616, 42.40388265670192], ('age', 'float64'): [16.11809565095831, 16.11809565095831, 16.11809565095831, 16.11809565095831, 16.11809565095831, 16.11809565095831], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [42.478696420401704, 42.478696420401704, 42.478696420401704, 42.478696420401704, 42.478696420401704, 42.478696420401704], ('weight(t)', 'float64'): [2.1379586186848787, 1.3169578969248166, 2.0634370688955608, 1.1929107309930491, 2.42582831808226, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_acosh_multiple(self):
        df = self.df.copy()
        result = df.cols.acosh(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [2.1379586186848787, 1.3169578969248166, 2.0634370688955608, 1.1929107309930491, 2.42582831808226, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_acosh_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.acosh(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, 3.5254943480781717, 3.95087369077445, 3.2566139548000526, nan, 6.396926877426794]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_acosh_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.acosh(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asin(self):
        df = self.create_dataframe(data={('asin_test', 'object'): [1, '0', 10, nan, inf, None]}, force_data_types=True)
        result = df.cols.asin(cols=['asin_test'])
        expected = self.create_dataframe(data={('asin_test', 'float64'): [1.5707963267948966, 0.0, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asin_all(self):
        df = self.df.copy()
        result = df.cols.asin(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [nan, nan, nan, nan, nan, nan], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [nan, nan, nan, nan, nan, nan], ('Cybertronian', 'float64'): [1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 0.0], ('Date Type', 'float64'): [nan, nan, nan, nan, nan, nan], ('age', 'float64'): [nan, nan, nan, nan, nan, nan], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [nan, nan, nan, nan, nan, nan], ('weight(t)', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asin_multiple(self):
        df = self.df.copy()
        result = df.cols.asin(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asin_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.asin(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asin_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.asin(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asinh(self):
        df = self.create_dataframe(data={('asinh_test', 'float64'): [nan, nan, 1.0, -0.34, inf, 13.0]}, force_data_types=True)
        result = df.cols.asinh(cols=['asinh_test'])
        expected = self.create_dataframe(data={('asinh_test', 'float64'): [nan, nan, 0.881373587019543, -0.3337683516458822, inf, 3.2595725562629214]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asinh_all(self):
        df = self.df.copy()
        result = df.cols.asinh(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-4.025670415869822, 3.5272244561999657, 3.9516133360820653, 3.2595725562629214, nan, 6.39693243298235], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [2.99822295029797, 2.644120761058629, 2.644120761058629, 2.7764722807237177, 2.99822295029797, 2.7764722807237177], ('Cybertronian', 'float64'): [0.881373587019543, 0.881373587019543, 0.881373587019543, 0.881373587019543, 0.881373587019543, 0.0], ('Date Type', 'float64'): [42.5272960316005, 42.50374179977561, 42.478696420401704, 42.45597170709963, 42.429813915865616, 42.40388265670192], ('age', 'float64'): [16.11809565095833, 16.11809565095833, 16.11809565095833, 16.11809565095833, 16.11809565095833, 16.11809565095833], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [42.478696420401704, 42.478696420401704, 42.478696420401704, 42.478696420401704, 42.478696420401704, 42.478696420401704], ('weight(t)', 'float64'): [2.1650167641453284, 1.4436354751788103, 2.0947125472611012, 1.3504407402749725, 2.44122070725561, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asinh_multiple(self):
        df = self.df.copy()
        result = df.cols.asinh(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [2.1650167641453284, 1.4436354751788103, 2.0947125472611012, 1.3504407402749725, 2.44122070725561, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asinh_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.asinh(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-4.025670415869822, 3.5272244561999657, 3.9516133360820653, 3.2595725562629214, nan, 6.39693243298235]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_asinh_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.asinh(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atan(self):
        df = self.create_dataframe(data={('atan_test', 'object'): [1, '0', 11, nan, inf, None]}, force_data_types=True)
        result = df.cols.atan(cols=['atan_test'])
        expected = self.create_dataframe(data={('atan_test', 'float64'): [0.7853981633974483, 0.0, 1.4801364395941514, nan, 1.5707963267948966, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atan_all(self):
        df = self.df.copy()
        result = df.cols.atan(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-1.5350972141155728, 1.512040504079174, 1.5323537367737086, 1.4940244355251187, nan, 1.56746300580716], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [1.4711276743037347, 1.4288992721907328, 1.4288992721907328, 1.446441332248135, 1.4711276743037347, 1.446441332248135], ('Cybertronian', 'float64'): [0.7853981633974483, 0.7853981633974483, 0.7853981633974483, 0.7853981633974483, 0.7853981633974483, 0.0], ('Date Type', 'float64'): [1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966], ('age', 'float64'): [1.5707961267948967, 1.5707961267948967, 1.5707961267948967, 1.5707961267948967, 1.5707961267948967, 1.5707961267948967], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966, 1.5707963267948966], ('weight(t)', 'float64'): [1.3422996875030344, 1.1071487177940904, 1.3258176636680326, 1.0636978224025597, 1.3971251284533228, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atan_multiple(self):
        df = self.df.copy()
        result = df.cols.atan(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [1.3422996875030344, 1.1071487177940904, 1.3258176636680326, 1.0636978224025597, 1.3971251284533228, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atan_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.atan(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-1.5350972141155728, 1.512040504079174, 1.5323537367737086, 1.4940244355251187, nan, 1.56746300580716]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atan_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.atan(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atanh(self):
        df = self.create_dataframe(data={('atanh_test', 'float64'): [nan, nan, 9.0, -703.0, -inf, 0.0]}, force_data_types=True)
        result = df.cols.atanh(cols=['atanh_test'])
        expected = self.create_dataframe(data={('atanh_test', 'float64'): [nan, nan, nan, nan, nan, 0.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atanh_all(self):
        df = self.df.copy()
        result = df.cols.atanh(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [nan, nan, nan, nan, nan, nan], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [nan, nan, nan, nan, nan, nan], ('Cybertronian', 'float64'): [inf, inf, inf, inf, inf, 0.0], ('Date Type', 'float64'): [nan, nan, nan, nan, nan, nan], ('age', 'float64'): [nan, nan, nan, nan, nan, nan], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [nan, nan, nan, nan, nan, nan], ('weight(t)', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atanh_multiple(self):
        df = self.df.copy()
        result = df.cols.atanh(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atanh_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.atanh(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_atanh_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.atanh(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cos(self):
        df = self.create_dataframe(data={('cos_test', 'float64'): [3.151592, nan, 78.0, 0.0, inf, -12.0]}, force_data_types=True)
        result = df.cols.cos(cols=['cos_test'])
        expected = self.create_dataframe(data={('cos_test', 'float64'): [-0.9999500069522407, nan, -0.8578030932449878, 1.0, nan, 0.8438539587324921]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cos_all(self):
        df = self.df.copy()
        result = df.cols.cos(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-0.9626058663135666, -0.27516333805159693, 0.6469193223286404, 0.9074467814501962, nan, -0.022096619278683942], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [-0.8390715290764524, 0.7539022543433046, 0.7539022543433046, -0.14550003380861354, -0.8390715290764524, -0.14550003380861354], ('Cybertronian', 'float64'): [0.5403023058681398, 0.5403023058681398, 0.5403023058681398, 0.5403023058681398, 0.5403023058681398, 1.0], ('Date Type', 'float64'): [0.9389507026950656, -0.9897935466735063, -0.6638500853796738, -0.4576431097589368, 0.953692953031811, 0.45261672525172086], ('age', 'float64'): [-0.21532488687824783, -0.21532488687824783, -0.21532488687824783, -0.21532488687824783, -0.21532488687824783, -0.21532488687824783], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [-0.6638500853796738, -0.6638500853796738, -0.6638500853796738, -0.6638500853796738, -0.6638500853796738, -0.6638500853796738], ('weight(t)', 'float64'): [-0.40079917207997545, -0.4161468365471424, -0.6536436208636119, -0.2272020946930871, 0.8347127848391598, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cos_multiple(self):
        df = self.df.copy()
        result = df.cols.cos(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [-0.40079917207997545, -0.4161468365471424, -0.6536436208636119, -0.2272020946930871, 0.8347127848391598, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cos_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.cos(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-0.9626058663135666, -0.27516333805159693, 0.6469193223286404, 0.9074467814501962, nan, -0.022096619278683942]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cos_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.cos(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cosh(self):
        df = self.create_dataframe(data={('cosh_test', 'object'): [inf, '-3.141592', 2.7182, 0, None, -5000]}, force_data_types=True)
        result = df.cols.cosh(cols=['cosh_test'])
        expected = self.create_dataframe(data={('cosh_test', 'float64'): [inf, 11.59194572738583, 7.609507839025955, 1.0, nan, inf]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cosh_all(self):
        df = self.df.copy()
        result = df.cols.cosh(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [723128532145.7375, 12077476.37678767, 97864804714.41939, 221206.6960055904, nan, 9.712131976206279e+129], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [11013.232920103323, 548.3170351552121, 548.3170351552121, 1490.479161252178, 11013.232920103323, 1490.479161252178], ('Cybertronian', 'float64'): [1.5430806348152437, 1.5430806348152437, 1.5430806348152437, 1.5430806348152437, 1.5430806348152437, 1.0], ('Date Type', 'float64'): [inf, inf, inf, inf, inf, inf], ('age', 'float64'): [inf, inf, inf, inf, inf, inf], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [inf, inf, inf, inf, inf, inf], ('weight(t)', 'float64'): [36.85668112930399, 3.7621956910836314, 27.308232836016487, 3.1074731763172667, 149.43537346625888, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cosh_multiple(self):
        df = self.df.copy()
        result = df.cols.cosh(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [36.85668112930399, 3.7621956910836314, 27.308232836016487, 3.1074731763172667, 149.43537346625888, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cosh_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.cosh(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [723128532145.7375, 12077476.37678767, 97864804714.41939, 221206.6960055904, nan, 9.712131976206279e+129]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cosh_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.cosh(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sin(self):
        df = self.create_dataframe(data={('sin_test', 'float64'): [3.151592, nan, 320.0, 0.0, -inf, -10.0]}, force_data_types=True)
        result = df.cols.sin(cols=['sin_test'])
        expected = self.create_dataframe(data={('sin_test', 'float64'): [-0.009999179777050457, nan, -0.42815542808445156, 0.0, nan, 0.5440211108893698]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sin_all(self):
        df = self.df.copy()
        result = df.cols.sin(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-0.27090578830786904, -0.9613974918795568, 0.7625584504796027, 0.4201670368266409, nan, -0.9997558399011495], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [-0.5440211108893698, 0.6569865987187891, 0.6569865987187891, 0.9893582466233818, -0.5440211108893698, 0.9893582466233818], ('Cybertronian', 'float64'): [0.8414709848078965, 0.8414709848078965, 0.8414709848078965, 0.8414709848078965, 0.8414709848078965, 0.0], ('Date Type', 'float64'): [0.344051708189979, -0.14250871890337624, -0.7478656725250864, 0.8891359761533495, -0.30078189994988713, 0.891705164290534], ('age', 'float64'): [-0.9765424686570829, -0.9765424686570829, -0.9765424686570829, -0.9765424686570829, -0.9765424686570829, -0.9765424686570829], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [-0.7478656725250864, -0.7478656725250864, -0.7478656725250864, -0.7478656725250864, -0.7478656725250864, -0.7478656725250864], ('weight(t)', 'float64'): [-0.9161659367494549, 0.9092974268256817, -0.7568024953079282, 0.9738476308781951, -0.5506855425976376, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sin_multiple(self):
        df = self.df.copy()
        result = df.cols.sin(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [-0.9161659367494549, 0.9092974268256817, -0.7568024953079282, 0.9738476308781951, -0.5506855425976376, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sin_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.sin(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-0.27090578830786904, -0.9613974918795568, 0.7625584504796027, 0.4201670368266409, nan, -0.9997558399011495]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sin_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.sin(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sinh(self):
        df = self.create_dataframe(data={('sinh_test', 'object'): [inf, '3.141592', -2.7182, 0, None, 5000]}, force_data_types=True)
        result = df.cols.sinh(cols=['sinh_test'])
        expected = self.create_dataframe(data={('sinh_test', 'float64'): [inf, 1.8622955450675873, -1.72535430656715, 0.0, nan, 9.210340381976183]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sinh_all(self):
        df = self.df.copy()
        result = df.cols.sinh(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-4.025670415869822, 3.5272244561999657, 3.9516133360820653, 3.2595725562629214, nan, 6.39693243298235], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [2.99822295029797, 2.644120761058629, 2.644120761058629, 2.7764722807237177, 2.99822295029797, 2.7764722807237177], ('Cybertronian', 'float64'): [0.881373587019543, 0.881373587019543, 0.881373587019543, 0.881373587019543, 0.881373587019543, 0.0], ('Date Type', 'float64'): [42.5272960316005, 42.50374179977561, 42.478696420401704, 42.45597170709963, 42.429813915865616, 42.40388265670192], ('age', 'float64'): [16.11809565095833, 16.11809565095833, 16.11809565095833, 16.11809565095833, 16.11809565095833, 16.11809565095833], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [42.478696420401704, 42.478696420401704, 42.478696420401704, 42.478696420401704, 42.478696420401704, 42.478696420401704], ('weight(t)', 'float64'): [2.1650167641453284, 1.4436354751788103, 2.0947125472611012, 1.3504407402749725, 2.44122070725561, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sinh_multiple(self):
        df = self.df.copy()
        result = df.cols.sinh(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [2.1650167641453284, 1.4436354751788103, 2.0947125472611012, 1.3504407402749725, 2.44122070725561, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sinh_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.sinh(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-4.025670415869822, 3.5272244561999657, 3.9516133360820653, 3.2595725562629214, nan, 6.39693243298235]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_sinh_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.sinh(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tan(self):
        df = self.create_dataframe(data={('tan_test', 'float64'): [3.151592, nan, 91.0, 0.0, -inf, -15.0]}, force_data_types=True)
        result = df.cols.tan(cols=['tan_test'])
        expected = self.create_dataframe(data={('tan_test', 'float64'): [0.009999679691514851, nan, -0.10658787210537021, 0.0, nan, 0.8559934009085188]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tan_all(self):
        df = self.df.copy()
        result = df.cols.tan(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [0.28142960456426525, 3.49391564547484, 1.1787535542062797, 0.4630211329364896, nan, 45.244742070819356], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [0.6483608274590866, 0.8714479827243188, 0.8714479827243188, -6.799711455220379, 0.6483608274590866, -6.799711455220379], ('Cybertronian', 'float64'): [1.5574077246549023, 1.5574077246549023, 1.5574077246549023, 1.5574077246549023, 1.5574077246549023, 0.0], ('Date Type', 'float64'): [0.36642148219544335, 0.14397822594653087, 1.1265580723656332, -1.942858872324553, -0.315386518264285, 1.9701109449603655], ('age', 'float64'): [4.535204837744807, 4.535204837744807, 4.535204837744807, 4.535204837744807, 4.535204837744807, 4.535204837744807], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.1265580723656332, 1.1265580723656332, 1.1265580723656332, 1.1265580723656332, 1.1265580723656332, 1.1265580723656332], ('weight(t)', 'float64'): [2.28584787736698, -2.185039863261519, 1.1578212823495777, -4.286261674628062, -0.6597305715207762, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tan_multiple(self):
        df = self.df.copy()
        result = df.cols.tan(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [2.28584787736698, -2.185039863261519, 1.1578212823495777, -4.286261674628062, -0.6597305715207762, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tan_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.tan(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [0.28142960456426525, 3.49391564547484, 1.1787535542062797, 0.4630211329364896, nan, 45.244742070819356]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tan_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.tan(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tanh(self):
        df = self.create_dataframe(data={('tanh_test', 'object'): [-inf, '3.141592', 2.7182, -1, None, 5000]}, force_data_types=True)
        result = df.cols.tanh(cols=['tanh_test'])
        expected = self.create_dataframe(data={('tanh_test', 'float64'): [-1.0, 0.9962720713567641, 0.9913275027555555, -0.7615941559557649, nan, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tanh_all(self):
        df = self.df.copy()
        result = df.cols.tanh(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-1.0, 0.9999999999999966, 1.0, 0.9999999999897818, nan, 1.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [0.9999999958776927, 0.9999983369439447, 0.9999983369439447, 0.9999997749296758, 0.9999999958776927, 0.9999997749296758], ('Cybertronian', 'float64'): [0.7615941559557649, 0.7615941559557649, 0.7615941559557649, 0.7615941559557649, 0.7615941559557649, 0.0], ('Date Type', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 1.0], ('age', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 1.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 1.0], ('weight(t)', 'float64'): [0.9996318561900731, 0.9640275800758169, 0.999329299739067, 0.9468060128462683, 0.9999776092809898, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tanh_multiple(self):
        df = self.df.copy()
        result = df.cols.tanh(cols=['NullType', 'weight(t)', 'japanese name'], output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'float64'): [0.9996318561900731, 0.9640275800758169, 0.999329299739067, 0.9468060128462683, 0.9999776092809898, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tanh_numeric(self):
        df = self.df.copy().cols.select(['height(ft)'])
        result = df.cols.tanh(cols=['height(ft)'])
        expected = self.create_dataframe(data={('height(ft)', 'float64'): [-1.0, 0.9999999999999966, 1.0, 0.9999999999897818, nan, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_tanh_string(self):
        df = self.df.copy().cols.select(['names'])
        result = df.cols.tanh(cols=['names'], output_cols=['names_2'])
        expected = self.create_dataframe(data={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))


class TestTrigonometricDask(TestTrigonometricPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestTrigonometricPartitionDask(TestTrigonometricPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestTrigonometricCUDF(TestTrigonometricPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestTrigonometricDC(TestTrigonometricPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestTrigonometricPartitionDC(TestTrigonometricPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestTrigonometricSpark(TestTrigonometricPandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestTrigonometricVaex(TestTrigonometricPandas):
        config = {'engine': 'vaex'}
