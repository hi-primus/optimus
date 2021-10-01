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


class TestDatePandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}
    maxDiff = None

    def test_cols_date_format_all(self):
        df = self.df
        result = df.cols.date_format(cols='*')
        expected = {'date arrival': '%a %b %d %Y %H:%M:%S -%z', 'some date': '%a %b %d %Y %H:%M:%S -%z', 'last date seen': '%Y/%d/%m', 'Date Type': True, 'timestamp': True}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_date_format_multiple(self):
        df = self.df
        result = df.cols.date_format(cols=['Date Type', 'timestamp', 'last date seen'])
        expected = {'Date Type': True, 'timestamp': True, 'last date seen': '%Y/%d/%m'}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_date_format_single(self):
        df = self.df
        result = df.cols.date_format(cols=['date arrival'])
        expected = '%a %b %d %Y %H:%M:%S -%z'
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_day_all(self):
        df = self.df
        result = df.cols.day(cols='*')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'int64'): [9, 8, 12, 6, 5, 5], ('Date Type', 'int64'): [10, 10, 1, 24, 10, 30], ('timestamp', 'int64'): [24, 24, 24, 24, 5, 3]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_day_multiple(self):
        df = self.df
        result = df.cols.day(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [9, 8, 12, 6, 5, 5], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [10, 10, 1, 24, 10, 30], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [24, 24, 24, 24, 5, 3]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_day_single(self):
        df = self.df
        result = df.cols.day(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [30.0, 1.0, 30.0, 31.0, nan, 31.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_all(self):
        df = self.df
        result = df.cols.days_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        expected = self.create_dataframe(data={('date arrival', 'int64'): [-8674, -7792, -7913, -1, -13895, 15858], ('some date', 'int64'): [-1, -13895, -7792, -8674, 15858, -7913], ('last date seen', 'int64'): [-6097, -5700, -8016, -4909, -4513, -132], ('Date Type', 'int64'): [-6097, -5700, -7365, -4923, -4513, -7912], ('timestamp', 'int64'): [-5471, -5289, -5289, -5289, -7766, -7917]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_all_today(self):
        df = self.df
        result = df.cols.days_between(cols='*', round='up')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-729.0, 153.0, 32.0, 7944.0, -5950.0, 23803.0], ('some date', 'float64'): [7944.0, -5950.0, 153.0, -729.0, 23803.0, 32.0], ('last date seen', 'float64'): [1847.0, 2244.0, -72.0, 3035.0, 3431.0, 7812.0], ('Date Type', 'float64'): [1847.0, 2244.0, 579.0, 3021.0, 3431.0, 32.0], ('timestamp', 'float64'): [2473.0, 2656.0, 2656.0, 2655.0, 179.0, 28.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_multiple(self):
        df = self.df
        result = df.cols.days_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-1655.0, -1258.0, -3574.0, -467.0, -71.0, 4310.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-1655.0, -1258.0, -2923.0, -481.0, -71.0, -3470.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-1029.0, -847.0, -847.0, -847.0, -3324.0, -3475.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_multiple_today(self):
        df = self.df
        result = df.cols.days_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [1847.0, 2244.0, -72.0, 3035.0, 3431.0, 7812.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [1847.0, 2244.0, 579.0, 3021.0, 3431.0, 32.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [2473.0, 2656.0, 2656.0, 2655.0, 179.0, 28.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_single(self):
        df = self.df
        result = df.cols.days_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S %z', round='down')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-757.0, 125.0, 5.0, 7917.0, -5978.0, 23775.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_single_timezone(self):
        df = self.df
        result = df.cols.days_between(cols=['date arrival', 'some date'], round='round', date_format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('date arrival_some date', 'float64'): [-8674.0, nan, -121.0, 8673.0, -29753.0, 23770.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_two_cols(self):
        df = self.df
        result = df.cols.days_between(cols=['date arrival'], value=['timestamp'], round=False, output_cols=['days'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('days', 'int64'): [-3203, -2504, -2625, 5288, -6129, 23774], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_format_date_all(self):
        df = self.df
        result = df.cols.format_date(cols='*', output_format='%d')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['09', '08', '12', '06', '05', '05'], ('Date Type', 'object'): ['10', '10', '01', '24', '10', '30'], ('timestamp', 'object'): ['24', '24', '24', '24', '05', '03']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_format_date_multiple(self):
        df = self.df
        result = df.cols.format_date(cols=['Date Type', 'timestamp', 'last date seen'], output_format='%H:%M:%S', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'object'): ['00:00:00', '00:00:00', '00:00:00', '00:00:00', '00:00:00', '00:00:00'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'object'): ['00:00:00', '00:00:00', '00:00:00', '00:00:00', '00:00:00', '00:00:00'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'object'): ['00:00:00', '15:00:00', '02:12:00', '23:03:59', '17:38:11', '00:00:01']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_format_date_single(self):
        df = self.df
        result = df.cols.format_date(cols=['date arrival'], current_format='%a %b %d %Y %H:%M:%S %z', output_format='%d/%Y/%m')
        expected = self.create_dataframe(data={('date arrival', 'object'): ['30/2023/09', '01/2021/05', '30/2021/08', '31/1999/12', nan, '31/1956/07'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hour_all(self):
        df = self.df
        result = df.cols.hour(cols='*')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'int64'): [0, 15, 2, 23, 17, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hour_multiple(self):
        df = self.df
        result = df.cols.hour(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [0, 15, 2, 23, 17, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hour_single(self):
        df = self.df
        result = df.cols.hour(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [11.0, 13.0, 0.0, 23.0, nan, 12.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_all(self):
        df = self.df
        result = df.cols.hours_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-208167.6363888889, -187001.43083333335, -189892.00027777778, -3.9997222222222213, -333470.5052777778, 380600.0], ('some date', 'float64'): [-3.9997222222222213, -333470.5052777778, -187001.43083333335, -208167.6363888889, 380600.0, -189892.00027777778], ('last date seen', 'float64'): [-146328.0, -136800.0, -192384.0, -117816.0, -108312.0, -3168.0], ('Date Type', 'float64'): [-146328.0, -136800.0, -176760.0, -118152.0, -108312.0, -189888.0], ('timestamp', 'float64'): [-131304.0, -126927.0, -126914.2, -126935.0663888889, -186377.6363888889, -189984.00027777778]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_all_today(self):
        df = self.df
        result = df.cols.hours_between(cols='*', round='up')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-17493.0, 3673.0, 783.0, 190671.0, -142796.0, 571275.0], ('some date', 'float64'): [190671.0, -142796.0, 3673.0, -17493.0, 571275.0, 783.0], ('last date seen', 'float64'): [44347.0, 53875.0, -1709.0, 72859.0, 82363.0, 187507.0], ('Date Type', 'float64'): [44347.0, 53875.0, 13915.0, 72523.0, 82363.0, 787.0], ('timestamp', 'float64'): [59371.0, 63748.0, 63761.0, 63740.0, 4297.0, 691.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_multiple(self):
        df = self.df
        result = df.cols.hours_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-39720.0, -30192.0, -85776.0, -11208.0, -1704.0, 103440.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-39720.0, -30192.0, -70152.0, -11544.0, -1704.0, -83280.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-24696.0, -20319.0, -20307.0, -20328.0, -79770.0, -83377.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_multiple_today(self):
        df = self.df
        result = df.cols.hours_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [44347.0, 53875.0, -1709.0, 72859.0, 82363.0, 187507.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [44347.0, 53875.0, 13915.0, 72523.0, 82363.0, 787.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [59371.0, 63748.0, 63761.0, 63740.0, 4297.0, 691.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_single(self):
        df = self.df
        result = df.cols.hours_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S %z', round='down')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-18146.0, 3020.0, 130.0, 190018.0, -143449.0, 570622.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_single_timezone(self):
        df = self.df
        result = df.cols.hours_between(cols=['date arrival', 'some date'], round='round')
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('date arrival_some date', 'float64'): [-208164.0, 146469.0, -2891.0, 208164.0, -714071.0, 570492.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_two_cols(self):
        df = self.df
        result = df.cols.hours_between(cols=['some date'], value=['last date seen'], round=False, output_cols=['hours'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('hours', 'float64'): [146324.00027777778, -196670.5052777778, 5382.569166666667, -90351.63638888889, 488912.0, -186724.00027777778], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minute_all(self):
        df = self.df
        result = df.cols.minute(cols='*')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'int64'): [0, 0, 12, 3, 38, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minute_multiple(self):
        df = self.df
        result = df.cols.minute(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [0, 0, 12, 3, 38, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minute_single(self):
        df = self.df
        result = df.cols.minute(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [38.0, 25.0, 0.0, 59.0, nan, 0.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_all(self):
        df = self.df
        result = df.cols.minutes_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-12490058.183333334, -11220085.85, -11393520.016666668, -239.98333333333335, -20008230.316666666, 22836000.0], ('some date', 'float64'): [-239.98333333333335, -20008230.316666666, -11220085.85, -12490058.183333334, 22836000.0, -11393520.016666668], ('last date seen', 'float64'): [-8779680.0, -8208000.0, -11543040.0, -7068960.0, -6498720.0, -190080.0], ('Date Type', 'float64'): [-8779680.0, -8208000.0, -10605600.0, -7089120.0, -6498720.0, -11393280.0], ('timestamp', 'float64'): [-7878240.0, -7615620.0, -7614852.0, -7616103.983333333, -11182658.183333334, -11399040.016666668]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_all_today(self):
        df = self.df
        result = df.cols.minutes_between(cols='*', round='up')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-1049604.0, 220368.0, 46934.0, 11440214.0, -8567777.0, 34276454.0], ('some date', 'float64'): [11440214.0, -8567777.0, 220368.0, -1049604.0, 34276454.0, 46934.0], ('last date seen', 'float64'): [2660774.0, 3232454.0, -102586.0, 4371494.0, 4941734.0, 11250374.0], ('Date Type', 'float64'): [2660774.0, 3232454.0, 834854.0, 4351334.0, 4941734.0, 47174.0], ('timestamp', 'float64'): [3562214.0, 3824834.0, 3825602.0, 3824350.0, 257796.0, 41414.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_multiple(self):
        df = self.df
        result = df.cols.minutes_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-2383200.0, -1811520.0, -5146560.0, -672480.0, -102240.0, 6206400.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-2383200.0, -1811520.0, -4209120.0, -692640.0, -102240.0, -4996800.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-1481760.0, -1219140.0, -1218372.0, -1219624.0, -4786179.0, -5002561.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_multiple_today(self):
        df = self.df
        result = df.cols.minutes_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [2660774.0, 3232454.0, -102586.0, 4371494.0, 4941734.0, 11250374.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [2660774.0, 3232454.0, 834854.0, 4351334.0, 4941734.0, 47174.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [3562214.0, 3824834.0, 3825602.0, 3824350.0, 257796.0, 41414.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_single(self):
        df = self.df
        result = df.cols.minutes_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S %z', round='down')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-1088733.0, 181239.0, 7805.0, 11401085.0, -8606905.0, 34237325.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_single_timezone(self):
        df = self.df
        result = df.cols.minutes_between(cols=['date arrival', 'some date'], round='round', date_format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('date arrival_some date', 'float64'): [-12489818.0, nan, -173434.0, 12489818.0, -42844230.0, 34229520.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_two_cols(self):
        df = self.df
        result = df.cols.minutes_between(cols=['Date Type'], value=['timestamp'], round=False, output_cols=['minutes'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('minutes', 'float64'): [-901440.0, -592380.0, -2990748.0, 526983.9833333333, 4683938.183333334, 5760.016666666666], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_month_all(self):
        df = self.df
        result = df.cols.month(cols='*')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'int64'): [10, 10, 12, 10, 10, 12], ('Date Type', 'int64'): [9, 8, 3, 6, 5, 8], ('timestamp', 'int64'): [12, 6, 6, 6, 4, 9]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_month_multiple(self):
        df = self.df
        result = df.cols.month(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [10, 10, 12, 10, 10, 12], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [9, 8, 3, 6, 5, 8], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [12, 6, 6, 6, 4, 9]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_month_single(self):
        df = self.df
        result = df.cols.month(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [9.0, 5.0, 8.0, 12.0, nan, 7.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_all(self):
        df = self.df
        result = df.cols.months_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-284.98326454341975, -256.0052567814534, -259.9806977556007, -0.03285488408386209, -456.51861434526376, 521.0127518018851], ('some date', 'float64'): [-0.03285488408386209, -456.51861434526376, -256.0052567814534, -284.98326454341975, 521.0127518018851, -259.9806977556007], ('last date seen', 'float64'): [-200.31622825930717, -187.27283927801392, -263.36475081623854, -161.284625967679, -148.27409187046962, -4.336844699069796], ('Date Type', 'float64'): [-200.31622825930717, -187.27283927801392, -241.9762212776443, -161.74459434485308, -148.27409187046962, -259.94784287151685], ('timestamp', 'float64'): [-179.7490708228095, -173.7694819195466, -173.7694819195466, -173.7694819195466, -255.151029795273, -260.11211729193616]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_all_today(self):
        df = self.df
        result = df.cols.months_between(cols='*', round='up')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-23.0, 6.0, 2.0, 261.0, -195.0, 783.0], ('some date', 'float64'): [261.0, -195.0, 6.0, -23.0, 783.0, 2.0], ('last date seen', 'float64'): [61.0, 74.0, -2.0, 100.0, 113.0, 257.0], ('Date Type', 'float64'): [61.0, 74.0, 20.0, 100.0, 113.0, 2.0], ('timestamp', 'float64'): [82.0, 88.0, 88.0, 88.0, 6.0, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_multiple(self):
        df = self.df
        result = df.cols.months_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-55.0, -42.0, -118.0, -16.0, -3.0, 141.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-55.0, -42.0, -97.0, -16.0, -3.0, -115.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-34.0, -28.0, -28.0, -28.0, -110.0, -115.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_multiple_today(self):
        df = self.df
        result = df.cols.months_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [61.0, 74.0, -2.0, 100.0, 113.0, 257.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [61.0, 74.0, 20.0, 100.0, 113.0, 2.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [82.0, 88.0, 88.0, 88.0, 6.0, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_single(self):
        df = self.df
        result = df.cols.months_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S %z', round='down')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-25.0, 4.0, 0.0, 260.0, -197.0, 781.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_single_timezone(self):
        df = self.df
        result = df.cols.months_between(cols=['date arrival', 'some date'], round='round')
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('date arrival_some date', 'float64'): [-285.0, 200.0, -4.0, 285.0, -978.0, 781.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_two_cols(self):
        df = self.df
        result = df.cols.months_between(cols=['last date seen'], value=['Date Type'], round=False, output_cols=['months'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('months', 'float64'): [0.0, 0.0, -21.38852953859422, 0.4599683771740693, 0.0, 255.61099817244707], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_second_all(self):
        df = self.df
        result = df.cols.second(cols='*')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'int64'): [0, 0, 0, 59, 11, 1]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_second_multiple(self):
        df = self.df
        result = df.cols.second(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 59, 11, 1]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_second_single(self):
        df = self.df
        result = df.cols.second(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [11.0, 51.0, 1.0, 59.0, nan, 0.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_all(self):
        df = self.df
        result = df.cols.seconds_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        expected = self.create_dataframe(data={('date arrival', 'int64'): [-749403491, -673205151, -683611201, -14399, -1200493819, 1370160000], ('some date', 'int64'): [-14399, -1200493819, -673205151, -749403491, 1370160000, -683611201], ('last date seen', 'int64'): [-526780800, -492480000, -692582400, -424137600, -389923200, -11404800], ('Date Type', 'int64'): [-526780800, -492480000, -636336000, -425347200, -389923200, -683596800], ('timestamp', 'int64'): [-472694400, -456937200, -456891120, -456966239, -670959491, -683942401]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_all_today(self):
        df = self.df
        result = df.cols.seconds_between(cols='*', round='up')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-62976299.0, 13222041.0, 2815991.0, 686412793.0, -514066627.0, 2056587192.0], ('some date', 'float64'): [686412794.0, -514066626.0, 13222042.0, -62976298.0, 2056587193.0, 2815992.0], ('last date seen', 'float64'): [159646393.0, 193947193.0, -6155207.0, 262289593.0, 296503993.0, 675022393.0], ('Date Type', 'float64'): [159646393.0, 193947193.0, 50091193.0, 261079993.0, 296503993.0, 2830393.0], ('timestamp', 'float64'): [213732793.0, 229489993.0, 229536073.0, 229460954.0, 15467702.0, 2484792.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_multiple(self):
        df = self.df
        result = df.cols.seconds_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-142992000.0, -108691200.0, -308793600.0, -40348800.0, -6134400.0, 372384000.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-142992000.0, -108691200.0, -252547200.0, -41558400.0, -6134400.0, -299808000.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-88905600.0, -73148400.0, -73102320.0, -73177439.0, -287170691.0, -300153601.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_multiple_today(self):
        df = self.df
        result = df.cols.seconds_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [159646393.0, 193947193.0, -6155207.0, 262289593.0, 296503993.0, 675022393.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [159646393.0, 193947193.0, 50091193.0, 261079993.0, 296503993.0, 2830393.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [213732793.0, 229489993.0, 229536073.0, 229460954.0, 15467702.0, 2484792.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_single(self):
        df = self.df
        result = df.cols.seconds_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S %z', round='down')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-65323952.0, 10874388.0, 468338.0, 684065140.0, -516414280.0, 2054239539.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_single_timezone(self):
        df = self.df
        result = df.cols.seconds_between(cols=['date arrival', 'some date'], round='round')
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('date arrival_some date', 'float64'): [-749389092.0, 527288668.0, -10406050.0, 749389092.0, -2570653819.0, 2053771201.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_two_cols(self):
        df = self.df
        result = df.cols.seconds_between(cols=['date arrival'], value=['Date Type'], round=False, output_cols=['seconds'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('seconds', 'int64'): [-222622691, -180725151, -47275201, 425332801, -810570619, 2053756800], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_weekday_all(self):
        df = self.df
        result = df.cols.weekday(cols='*')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'int64'): [6, 3, 6, 6, 4, 1], ('Date Type', 'int64'): [5, 0, 6, 0, 3, 0], ('timestamp', 'int64'): [2, 1, 1, 1, 0, 4]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_weekday_multiple(self):
        df = self.df
        result = df.cols.weekday(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [6, 3, 6, 6, 4, 1], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [5, 0, 6, 0, 3, 0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [2, 1, 1, 1, 0, 4]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_weekday_single(self):
        df = self.df
        result = df.cols.weekday(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [5.0, 5.0, 0.0, 4.0, nan, 1.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_year_all(self):
        df = self.df
        result = df.cols.year(cols='*')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [nan, nan, nan, nan, nan, nan], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'int64'): [2016, 2015, 2021, 2013, 2012, 2000], ('Date Type', 'int64'): [2016, 2015, 2020, 2013, 2012, 2021], ('timestamp', 'int64'): [2014, 2014, 2014, 2014, 2021, 2021]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_year_multiple(self):
        df = self.df
        result = df.cols.year(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [2016, 2015, 2021, 2013, 2012, 2000], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [2016, 2015, 2020, 2013, 2012, 2021], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [2014, 2014, 2014, 2014, 2021, 2021]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_year_single(self):
        df = self.df
        result = df.cols.year(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [2023.0, 2021.0, 2021.0, 1999.0, nan, 1956.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_all(self):
        df = self.df
        result = df.cols.years_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-23.748117727583846, -21.333333333333332, -21.66461327857632, -0.0027378507871321013, -38.04243668720055, 43.41683778234086], ('some date', 'float64'): [-0.0027378507871321013, -38.04243668720055, -21.333333333333332, -23.748117727583846, 43.41683778234086, -21.66461327857632], ('last date seen', 'float64'): [-16.692676249144423, -15.605749486652977, -21.946611909650922, -13.440109514031485, -12.355920602327172, -0.3613963039014374], ('Date Type', 'float64'): [-16.692676249144423, -15.605749486652977, -20.164271047227928, -13.478439425051334, -12.355920602327172, -21.661875427789184], ('timestamp', 'float64'): [-14.978781656399725, -14.480492813141684, -14.480492813141684, -14.480492813141684, -21.2621492128679, -21.675564681724847]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_all_today(self):
        df = self.df
        result = df.cols.years_between(cols='*', round='up')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-1.0, 1.0, 1.0, 22.0, -16.0, 66.0], ('some date', 'float64'): [22.0, -16.0, 1.0, -1.0, 66.0, 1.0], ('last date seen', 'float64'): [6.0, 7.0, -0.0, 9.0, 10.0, 22.0], ('Date Type', 'float64'): [6.0, 7.0, 2.0, 9.0, 10.0, 1.0], ('timestamp', 'float64'): [7.0, 8.0, 8.0, 8.0, 1.0, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_multiple(self):
        df = self.df
        result = df.cols.years_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-5.0, -4.0, -10.0, -2.0, -1.0, 11.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-5.0, -4.0, -9.0, -2.0, -1.0, -10.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-3.0, -3.0, -3.0, -3.0, -10.0, -10.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_multiple_today(self):
        df = self.df
        result = df.cols.years_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [6.0, 7.0, -0.0, 9.0, 10.0, 22.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [6.0, 7.0, 2.0, 9.0, 10.0, 1.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [7.0, 8.0, 8.0, 8.0, 1.0, 1.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_single(self):
        df = self.df
        result = df.cols.years_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S %z', round='down')
        expected = self.create_dataframe(data={('date arrival', 'float64'): [-3.0, 0.0, 0.0, 21.0, -17.0, 65.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_single_timezone(self):
        df = self.df
        result = df.cols.years_between(cols=['date arrival', 'some date'], round='round', date_format='%a %b %d %Y %H:%M:%S %z')
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('date arrival_some date', 'float64'): [-24.0, nan, -0.0, 24.0, -81.0, 65.0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_two_cols(self):
        df = self.df
        result = df.cols.years_between(cols=['date arrival'], value=['some date'], round=False, output_cols=['years'])
        expected = self.create_dataframe(data={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('years', 'float64'): [-23.748117727583846, 16.706365503080082, -0.33127994524298426, 23.745379876796715, -81.45927446954141, 65.07871321013005], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))


class TestDateDask(TestDatePandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestDatePartitionDask(TestDatePandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestDateCUDF(TestDatePandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestDateDC(TestDatePandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestDatePartitionDC(TestDatePandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestDateSpark(TestDatePandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestDateVaex(TestDatePandas):
        config = {'engine': 'vaex'}
