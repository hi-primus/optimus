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


class TestStatisticsPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None

    def test_cols_cummax_all(self):
        df = self.df.copy()
        result = df.cols.cummax(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 26.0, nan, 300.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [10.0, 10.0, 10.0, 10.0, 10.0, 10.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 1.0], ('Date Type', 'float64'): [1.4734656e+18, 1.4734656e+18, 1.4734656e+18, 1.4734656e+18, 1.4734656e+18, 1.4734656e+18], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18], ('weight(t)', 'float64'): [4.3, 4.3, 4.3, 4.3, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cummax_multiple(self):
        df = self.df.copy()
        result = df.cols.cummax(cols=['height(ft)', 'age', 'rank'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 26.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'float64'): [10.0, 10.0, 10.0, 10.0, 10.0, 10.0], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cummax_numeric(self):
        df = self.df.copy()
        result = df.cols.cummax(cols='weight(t)')
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 4.3, 4.3, 4.3, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cummin_all(self):
        df = self.df.copy()
        result = df.cols.cummin(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-28.0, -28.0, -28.0, -28.0, nan, -28.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [10.0, 7.0, 7.0, 7.0, 7.0, 7.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1.4734656e+18, 1.4391648e+18, 1.403568e+18, 1.372032e+18, 1.336608e+18, 1.3023936e+18], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18, 1.403568e+18], ('weight(t)', 'float64'): [4.3, 2.0, 2.0, 1.8, 1.8, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cummin_multiple(self):
        df = self.df.copy()
        result = df.cols.cummin(cols=['height(ft)', 'age', 'rank'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, -28.0, -28.0, -28.0, nan, -28.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'float64'): [10.0, 7.0, 7.0, 7.0, 7.0, 7.0], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cummin_numeric(self):
        df = self.df.copy()
        result = df.cols.cummin(cols='weight(t)')
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 2.0, 1.8, 1.8, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cumprod_all(self):
        df = self.df.copy()
        result = df.cols.cumprod(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-28.0, -476.0, -12376.0, -160888.0, nan, -48266400.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [10.0, 70.0, 490.0, 3920.0, 39200.0, 313600.0], ('Cybertronian', 'float64'): [1.0, 1.0, 1.0, 1.0, 1.0, 0.0], ('Date Type', 'float64'): [1.4734656e+18, 2.12055982553088e+36, 2.9763499132007264e+54, 4.083647324108619e+72, 5.458235682582173e+90, 7.108771220286654e+108], ('age', 'float64'): [5000000.0, 25000000000000.0, 1.25e+20, 6.25e+26, 3.125e+33, 1.5625e+40], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.403568e+18, 1.970003130624e+36, 2.7650333540436665e+54, 3.880912334668361e+72, 5.447124363745802e+90, 7.645409448973968e+108], ('weight(t)', 'float64'): [4.3, 8.6, 34.4, 61.92, 352.944, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cumprod_multiple(self):
        df = self.df.copy()
        result = df.cols.cumprod(cols=['height(ft)', 'age', 'rank'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, -476.0, -12376.0, -160888.0, nan, -48266400.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'float64'): [10.0, 70.0, 490.0, 3920.0, 39200.0, 313600.0], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'float64'): [5000000.0, 25000000000000.0, 1.25e+20, 6.25e+26, 3.125e+33, 1.5625e+40], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cumprod_numeric(self):
        df = self.df.copy()
        result = df.cols.cumprod(cols='weight(t)')
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 8.6, 34.4, 61.92, 352.944, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cumsum_all(self):
        df = self.df.copy()
        result = df.cols.cumsum(cols='*')
        expected = self.create_dataframe(data={('NullType', 'float64'): [nan, nan, nan, nan, nan, nan], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'float64'): [-28.0, -11.0, 15.0, 28.0, nan, 328.0], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('last position seen', 'float64'): [nan, nan, nan, nan, nan, nan], ('rank', 'float64'): [10.0, 17.0, 24.0, 32.0, 42.0, 50.0], ('Cybertronian', 'float64'): [1.0, 2.0, 3.0, 4.0, 5.0, 5.0], ('Date Type', 'float64'): [1.4734656e+18, 2.9126304e+18, 4.3161984e+18, 5.6882304e+18, 7.0248384e+18, 8.327232e+18], ('age', 'float64'): [5000000.0, 10000000.0, 15000000.0, 20000000.0, 25000000.0, 30000000.0], ('function', 'float64'): [nan, nan, nan, nan, nan, nan], ('names', 'float64'): [nan, nan, nan, nan, nan, nan], ('timestamp', 'float64'): [1.403568e+18, 2.807136e+18, 4.210704e+18, 5.614272e+18, 7.01784e+18, 8.421408e+18], ('weight(t)', 'float64'): [4.3, 6.3, 10.3, 12.100000000000001, 17.8, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cumsum_multiple(self):
        df = self.df.copy()
        result = df.cols.cumsum(cols=['height(ft)', 'age', 'rank'])
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, -11.0, 15.0, 28.0, nan, 328.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'float64'): [10.0, 17.0, 24.0, 32.0, 42.0, 50.0], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'float64'): [5000000.0, 10000000.0, 15000000.0, 20000000.0, 25000000.0, 30000000.0], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_cumsum_numeric(self):
        df = self.df.copy()
        result = df.cols.cumsum(cols='weight(t)')
        expected = self.create_dataframe(data={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 6.3, 10.3, 12.100000000000001, 17.8, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_kurtosis_all(self):
        df = self.df.copy()
        result = df.cols.kurtosis(cols='*')
        expected = {'NullType': nan, 'attributes': nan, 'date arrival': nan, 'function(binary)': nan, 'height(ft)': 0.1386336481508219, 'japanese name': nan, 'last date seen': nan, 'last position seen': nan, 'rank': -1.5000000000000002, 'Cybertronian': 1.200000000000001, 'Date Type': -1.2534316806077568, 'age': -3.0, 'function': nan, 'names': nan, 'timestamp': -3.0, 'weight(t)': -1.4364057698737658}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_kurtosis_multiple(self):
        df = self.df.copy()
        result = df.cols.kurtosis(cols=['height(ft)', 'age', 'rank'])
        expected = {'height(ft)': 0.1386336481508219, 'age': -3.0, 'rank': -1.5000000000000002}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_kurtosis_numeric(self):
        df = self.df.copy()
        result = df.cols.kurtosis(cols='weight(t)')
        expected = -1.4364057698737658
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_mad_all(self):
        df = self.df.copy()
        result = df.cols.mad(cols=['NullType', 'attributes', 'date arrival', 'function(binary)', 'height(ft)', 'japanese name', 'last date seen', 'last position seen', 'rank', 'Cybertronian', 'age', 'function', 'names', 'timestamp', 'weight(t)'], estimate=False)
        expected = {'NullType': nan, 'attributes': nan, 'date arrival': nan, 'function(binary)': nan, 'height(ft)': 9.0, 'japanese name': nan, 'last date seen': nan, 'last position seen': nan, 'rank': 1.0, 'Cybertronian': 0.0, 'age': 0.0, 'function': nan, 'names': nan, 'timestamp': 0.0, 'weight(t)': 1.7000000000000002}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_mad_multiple(self):
        df = self.df.copy()
        result = df.cols.mad(cols=['height(ft)', 'age', 'rank'], more=True, estimate=False)
        expected = {'height(ft)': {'mad': 9.0, 'median': 17.0}, 'age': {'mad': 0.0, 'median': 5000000.0}, 'rank': {'mad': 1.0, 'median': 8.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_mad_numeric(self):
        df = self.df.copy()
        result = df.cols.mad(cols='weight(t)', relative_error=0.45, estimate=False)
        expected = 1.7000000000000002
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_max_all(self):
        df = self.df.copy()
        result = df.cols.max(cols='*')
        expected = {'NullType': nan, 'attributes': '[None, 5700.0]', 'date arrival': '1980/04/10', 'function(binary)': "bytearray(b'Security')", 'height(ft)': 300.0, 'japanese name': "['Roadbuster']", 'last date seen': '2016/09/10', 'last position seen': '37.789563,-122.400356', 'rank': 10, 'Cybertronian': True, 'Date Type': datetime.datetime(2016, 9, 10, 0, 0), 'age': 5000000, 'function': 'Security', 'names': 'ironhide&', 'timestamp': datetime.datetime(2014, 6, 24, 0, 0), 'weight(t)': 5.7}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_max_multiple(self):
        df = self.df.copy()
        result = df.cols.max(cols=['height(ft)', 'age', 'rank'])
        expected = {'height(ft)': 300.0, 'age': 5000000.0, 'rank': 10.0}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_max_numeric(self):
        df = self.df.copy()
        result = df.cols.max(cols='weight(t)')
        expected = 5.7
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_mean_all(self):
        df = self.df.copy()
        result = df.cols.mean(cols='*')
        expected = {'NullType': nan, 'attributes': nan, 'date arrival': nan, 'function(binary)': nan, 'height(ft)': 65.6, 'japanese name': nan, 'last date seen': nan, 'last position seen': nan, 'rank': 8.333333333333334, 'Cybertronian': 0.8333333333333334, 'Date Type': 1.387872e+18, 'age': 5000000.0, 'function': nan, 'names': nan, 'timestamp': 1.403568e+18, 'weight(t)': 3.56}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_mean_multiple(self):
        df = self.df.copy()
        result = df.cols.mean(cols=['height(ft)', 'age', 'rank'])
        expected = {'height(ft)': 65.6, 'age': 5000000.0, 'rank': 8.333333333333334}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_mean_numeric(self):
        df = self.df.copy()
        result = df.cols.mean(cols='weight(t)')
        expected = 3.56
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_median_all(self):
        df = self.df.copy()
        result = df.cols.median(cols='*')
        expected = {'NullType': nan, 'attributes': nan, 'date arrival': nan, 'function(binary)': nan, 'height(ft)': {0.5: 17.0}, 'japanese name': nan, 'last date seen': nan, 'last position seen': nan, 'rank': {0.5: 8.0}, 'Cybertronian': {0.5: 1.0}, 'Date Type': {0.5: 1.3878e+18}, 'age': {0.5: 5000000.0}, 'function': nan, 'names': nan, 'timestamp': {0.5: 1.403568e+18}, 'weight(t)': {0.5: 4.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_median_multiple(self):
        df = self.df.copy()
        result = df.cols.median(cols=['height(ft)', 'age', 'rank'], relative_error=0.012)
        expected = {'height(ft)': {0.5: 17.0}, 'age': {0.5: 5000000.0}, 'rank': {0.5: 8.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_median_numeric(self):
        df = self.df.copy()
        result = df.cols.median(cols='weight(t)', relative_error=0.33)
        expected = None
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_min_all(self):
        df = self.df.copy()
        result = df.cols.min(cols='*')
        expected = {'NullType': nan, 'attributes': '[3.9624, 1800.0]', 'date arrival': '1980/04/10', 'function(binary)': "bytearray(b'Battle Station')", 'height(ft)': -28.0, 'japanese name': "['Bumble', 'Goldback']", 'last date seen': '2011/04/10', 'last position seen': '10.642707,-71.612534', 'rank': 7, 'Cybertronian': False, 'Date Type': datetime.datetime(2011, 4, 10, 0, 0), 'age': 5000000, 'function': 'Battle Station', 'names': 'Jazz', 'timestamp': datetime.datetime(2014, 6, 24, 0, 0), 'weight(t)': 1.8}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_min_multiple(self):
        df = self.df.copy()
        result = df.cols.min(cols=['height(ft)', 'age', 'rank'])
        expected = {'height(ft)': -28.0, 'age': 5000000.0, 'rank': 7.0}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_min_numeric(self):
        df = self.df.copy()
        result = df.cols.min(cols='weight(t)')
        expected = 1.8
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_mode_all(self):
        df = self.create_dataframe(data={('weight', 'float64'): [10.2, 10.2, 20.4, 20.5, 33.2, nan], ('height', 'float64'): [26.0, 17.0, 26.0, 17.0, nan, 300.0], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', nan], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, nan], ('rank', 'int64'): [10, 7, 7, 8, 10, 8]}, force_data_types=True)
        result = df.cols.mode(cols='*')
        expected = {'weight': 10.2, 'height': [17.0, 26.0], 'date arrival': '1980/04/10', 'age': 5000000.0, 'rank': [7, 8, 10]}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_mode_multiple(self):
        df = self.create_dataframe(data={('weight', 'float64'): [10.2, 10.2, 20.4, 20.5, 33.2, nan], ('height', 'float64'): [26.0, 17.0, 26.0, 17.0, nan, 300.0], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', nan], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, nan], ('rank', 'int64'): [10, 7, 7, 8, 10, 8]}, force_data_types=True)
        result = df.cols.mode(cols=['height', 'age', 'rank'])
        expected = {'height': [17.0, 26.0], 'age': 5000000.0, 'rank': [7, 8, 10]}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_mode_numeric(self):
        df = self.create_dataframe(data={('weight', 'float64'): [10.2, 10.2, 20.4, 20.5, 33.2, nan], ('height', 'float64'): [26.0, 17.0, 26.0, 17.0, nan, 300.0], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', nan], ('age', 'float64'): [5000000.0, 5000000.0, 5000000.0, 5000000.0, 5000000.0, nan], ('rank', 'int64'): [10, 7, 7, 8, 10, 8]}, force_data_types=True)
        result = df.cols.mode(cols='weight')
        expected = 10.2
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_percentile_all(self):
        df = self.df.copy()
        result = df.cols.percentile(cols='*', estimate=False)
        expected = {'NullType': nan, 'attributes': nan, 'date arrival': nan, 'function(binary)': nan, 'height(ft)': {0.25: 13.0, 0.5: 17.0, 0.75: 26.0}, 'japanese name': nan, 'last date seen': nan, 'last position seen': nan, 'rank': {0.25: 7.25, 0.5: 8.0, 0.75: 9.5}, 'Cybertronian': {0.25: 1.0, 0.5: 1.0, 0.75: 1.0}, 'Date Type': {0.25: 1.345464e+18, 0.5: 1.3878e+18, 0.75: 1.4302656e+18}, 'age': {0.25: 5000000.0, 0.5: 5000000.0, 0.75: 5000000.0}, 'function': nan, 'names': nan, 'timestamp': {0.25: 1.403568e+18, 0.5: 1.403568e+18, 0.75: 1.403568e+18}, 'weight(t)': {0.25: 2.0, 0.5: 4.0, 0.75: 4.3}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_percentile_multiple(self):
        df = self.df.copy()
        result = df.cols.percentile(cols=['height(ft)', 'age', 'rank'], values=0.95, relative_error=0.012, estimate=False)
        expected = {'height(ft)': 245.19999999999996, 'age': 5000000.0, 'rank': 10.0}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_percentile_numeric_multiple(self):
        df = self.df.copy()
        result = df.cols.percentile(cols='weight(t)', values=[0.25, 0.5, 0.75], estimate=False)
        expected = {0.25: 2.0, 0.5: 4.0, 0.75: 4.3}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_percentile_numeric_single(self):
        df = self.df.copy()
        result = df.cols.percentile(cols='weight(t)', values=0.3, estimate=False)
        expected = 2.4
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_range_all(self):
        df = self.df.copy()
        result = df.cols.range(cols='*')
        expected = {'NullType': {'min': nan, 'max': nan}, 'attributes': {'min': nan, 'max': nan}, 'date arrival': {'min': nan, 'max': nan}, 'function(binary)': {'min': nan, 'max': nan}, 'height(ft)': {'min': -28.0, 'max': 300.0}, 'japanese name': {'min': nan, 'max': nan}, 'last date seen': {'min': nan, 'max': nan}, 'last position seen': {'min': nan, 'max': nan}, 'rank': {'min': 7.0, 'max': 10.0}, 'Cybertronian': {'min': 0.0, 'max': 1.0}, 'Date Type': {'min': 1.3023936e+18, 'max': 1.4734656e+18}, 'age': {'min': 5000000.0, 'max': 5000000.0}, 'function': {'min': nan, 'max': nan}, 'names': {'min': nan, 'max': nan}, 'timestamp': {'min': 1.403568e+18, 'max': 1.403568e+18}, 'weight(t)': {'min': 1.8, 'max': 5.7}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_range_multiple(self):
        df = self.df.copy()
        result = df.cols.range(cols=['height(ft)', 'age', 'rank'])
        expected = {'height(ft)': {'min': -28.0, 'max': 300.0}, 'age': {'min': 5000000.0, 'max': 5000000.0}, 'rank': {'min': 7.0, 'max': 10.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_range_numeric(self):
        df = self.df.copy()
        result = df.cols.range(cols='weight(t)')
        expected = {'min': 1.8, 'max': 5.7}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_skew_all(self):
        df = self.df.copy()
        result = df.cols.skew(cols='*')
        expected = {'NullType': nan, 'attributes': nan, 'date arrival': nan, 'function(binary)': nan, 'height(ft)': 1.4048993602366326, 'japanese name': nan, 'last date seen': nan, 'last position seen': nan, 'rank': 0.38180177416060485, 'Cybertronian': -1.7888543819998322, 'Date Type': 0.002228190425762595, 'age': 0.0, 'function': nan, 'names': nan, 'timestamp': 0.0, 'weight(t)': 0.06521107849040725}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_skew_multiple(self):
        df = self.df.copy()
        result = df.cols.skew(cols=['height(ft)', 'age', 'rank'])
        expected = {'height(ft)': 1.4048993602366326, 'age': 0.0, 'rank': 0.38180177416060485}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_skew_numeric(self):
        df = self.df.copy()
        result = df.cols.skew(cols='weight(t)')
        expected = 0.06521107849040725
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_std_all(self):
        df = self.df.copy()
        result = df.cols.std(cols=['NullType', 'attributes', 'date arrival', 'function(binary)', 'height(ft)', 'japanese name', 'last date seen', 'last position seen', 'rank', 'Cybertronian', 'age', 'function', 'names', 'timestamp', 'weight(t)'])
        expected = {'NullType': nan, 'attributes': nan, 'date arrival': nan, 'function(binary)': nan, 'height(ft)': 132.66612227694, 'japanese name': nan, 'last date seen': nan, 'last position seen': nan, 'rank': 1.3662601021279464, 'Cybertronian': 0.408248290463863, 'age': 0.0, 'function': nan, 'names': nan, 'timestamp': 0.0, 'weight(t)': 1.6471186963907611}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_std_multiple(self):
        df = self.df.copy()
        result = df.cols.std(cols=['height(ft)', 'age', 'rank'])
        expected = {'height(ft)': 132.66612227694, 'age': 0.0, 'rank': 1.3662601021279464}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_std_numeric(self):
        df = self.df.copy()
        result = df.cols.std(cols='weight(t)')
        expected = 1.6471186963907611
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_sum_all(self):
        df = self.df.copy()
        result = df.cols.sum(cols='*')
        expected = {'NullType': 0.0, 'attributes': 0.0, 'date arrival': 0.0, 'function(binary)': 0.0, 'height(ft)': 328.0, 'japanese name': 0.0, 'last date seen': 0.0, 'last position seen': 0.0, 'rank': 50.0, 'Cybertronian': 5.0, 'Date Type': 8.327232e+18, 'age': 30000000.0, 'function': 0.0, 'names': 0.0, 'timestamp': 8.421408e+18, 'weight(t)': 17.8}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_sum_multiple(self):
        df = self.df.copy()
        result = df.cols.sum(cols=['height(ft)', 'age', 'rank'])
        expected = {'height(ft)': 328.0, 'age': 30000000.0, 'rank': 50.0}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_sum_numeric(self):
        df = self.df.copy()
        result = df.cols.sum(cols='weight(t)')
        expected = 17.8
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_var_all(self):
        df = self.df.copy()
        result = df.cols.var(cols=['NullType', 'attributes', 'date arrival', 'function(binary)', 'height(ft)', 'japanese name', 'last date seen', 'last position seen', 'rank', 'Cybertronian', 'age', 'function', 'names', 'timestamp', 'weight(t)'])
        expected = {'NullType': nan, 'attributes': nan, 'date arrival': nan, 'function(binary)': nan, 'height(ft)': 17600.3, 'japanese name': nan, 'last date seen': nan, 'last position seen': nan, 'rank': 1.8666666666666665, 'Cybertronian': 0.16666666666666669, 'age': 0.0, 'function': nan, 'names': nan, 'timestamp': 0.0, 'weight(t)': 2.713}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_var_multiple(self):
        df = self.df.copy()
        result = df.cols.var(cols=['height(ft)', 'age', 'rank'])
        expected = {'height(ft)': 17600.3, 'age': 0.0, 'rank': 1.8666666666666665}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_var_numeric(self):
        df = self.df.copy()
        result = df.cols.var(cols='weight(t)')
        expected = 2.713
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))


class TestStatisticsDask(TestStatisticsPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestStatisticsPartitionDask(TestStatisticsPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStatisticsCUDF(TestStatisticsPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStatisticsDC(TestStatisticsPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStatisticsPartitionDC(TestStatisticsPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStatisticsSpark(TestStatisticsPandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestStatisticsVaex(TestStatisticsPandas):
        config = {'engine': 'vaex'}
