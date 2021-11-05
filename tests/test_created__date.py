import datetime
from optimus.tests.base import TestBase
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort, df_dicts_equal, results_equal


def Timestamp(t):
    return datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")


nan = float("nan")
inf = float("inf")


class TestDatePandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}
    maxDiff = None

    def test_cols_date_format_all(self):
        df = self.df
        result = df.cols.date_format(cols='*')
        expected = {'date arrival': '%a %b %d %Y %H:%M:%S -%Y', 'some date': '%a %b %d %Y %H:%M:%S -%Y', 'last date seen': '%Y/%d/%m', 'Date Type': True, 'timestamp': True}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_date_format_multiple(self):
        df = self.df
        result = df.cols.date_format(cols=['Date Type', 'timestamp', 'last date seen'])
        expected = {'Date Type': True, 'timestamp': True, 'last date seen': '%Y/%d/%m'}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_date_format_single(self):
        df = self.df
        result = df.cols.date_format(cols=['date arrival'])
        expected = %a %b %d %Y %H:%M:%S -%Y
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_day_all(self):
        df = self.df
        result = df.cols.day(cols='*')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [30, 1, 30, 31, 15, 31], ('some date', 'int64'): [31, 15, 1, 30, 31, 30], ('last date seen', 'int64'): [9, 8, 12, 6, 5, 5], ('Date Type', 'int64'): [10, 10, 1, 24, 10, 30], ('timestamp', 'int64'): [24, 24, 24, 24, 5, 3]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_day_multiple(self):
        df = self.df
        result = df.cols.day(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [9, 8, 12, 6, 5, 5], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [10, 10, 1, 24, 10, 30], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [24, 24, 24, 24, 5, 3]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_day_single(self):
        df = self.df
        result = df.cols.day(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S -%Y')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [30, 1, 30, 31, 15, 31], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_all(self):
        df = self.df
        result = df.cols.days_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_all_today(self):
        df = self.df
        result = df.cols.days_between(cols='*', round='up')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_multiple(self):
        df = self.df
        result = df.cols.days_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-1655.0, -1258.0, -3574.0, -467.0, -71.0, 4310.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-1655.0, -1258.0, -2923.0, -481.0, -71.0, -3470.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-1029.0, -847.0, -847.0, -847.0, -3324.0, -3475.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_multiple_today(self):
        df = self.df
        result = df.cols.days_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [1819.0, 2216.0, -100.0, 3007.0, 3403.0, 7784.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [1819.0, 2216.0, 551.0, 2993.0, 3403.0, 4.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [2445.0, 2628.0, 2628.0, 2627.0, 151.0, 0.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_single(self):
        df = self.df
        result = df.cols.days_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S -%Y', round='down')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_days_between_single_timezone(self):
        df = self.df
        result = df.cols.days_between(cols=['date arrival', 'some date'], round='round')
        expected = self.create_dataframe(dict={('date arrival', 'float64'): [-8674.0, 6102.0, -121.0, 8673.0, -29753.0, 23770.0], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_format_date_all(self):
        df = self.df
        result = df.cols.format_date(cols='*', output_format='%d')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_format_date_multiple(self):
        df = self.df
        result = df.cols.format_date(cols=['Date Type', 'timestamp', 'last date seen'], output_format='%H:%M:%S', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'object'): ['00:00:00', '00:00:00', '00:00:00', '00:00:00', '00:00:00', '00:00:00'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'object'): ['00:00:00', '00:00:00', '00:00:00', '00:00:00', '00:00:00', '00:00:00'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'object'): ['00:00:00', '15:00:00', '02:12:00', '23:03:59', '17:38:11', '00:00:01']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_format_date_single(self):
        df = self.df
        result = df.cols.format_date(cols=['date arrival'], current_format='%a %b %d %Y %H:%M:%S -%Y', output_format='%d/%Y/%m')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hour_all(self):
        df = self.df
        result = df.cols.hour(cols='*')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [11, 13, 0, 23, 10, 12], ('some date', 'int64'): [23, 10, 13, 11, 12, 0], ('last date seen', 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'int64'): [0, 15, 2, 23, 17, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hour_multiple(self):
        df = self.df
        result = df.cols.hour(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [0, 15, 2, 23, 17, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hour_single(self):
        df = self.df
        result = df.cols.hour(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S -%Y')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [11, 13, 0, 23, 10, 12], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_all(self):
        df = self.df
        result = df.cols.hours_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_all_today(self):
        df = self.df
        result = df.cols.hours_between(cols='*', round='up')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_multiple(self):
        df = self.df
        result = df.cols.hours_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-39720.0, -30192.0, -85776.0, -11208.0, -1704.0, 103440.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-39720.0, -30192.0, -70152.0, -11544.0, -1704.0, -83280.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-24696.0, -20319.0, -20307.0, -20328.0, -79770.0, -83377.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_multiple_today(self):
        df = self.df
        result = df.cols.hours_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [43675.0, 53203.0, -2381.0, 72187.0, 81691.0, 186835.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [43675.0, 53203.0, 13243.0, 71851.0, 81691.0, 115.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [58699.0, 63076.0, 63089.0, 63068.0, 3626.0, 19.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_single(self):
        df = self.df
        result = df.cols.hours_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S -%Y', round='down')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_hours_between_single_timezone(self):
        df = self.df
        result = df.cols.hours_between(cols=['date arrival', 'some date'], round='round')
        expected = self.create_dataframe(dict={('date arrival', 'float64'): [-208164.0, 146469.0, -2891.0, 208164.0, -714071.0, 570492.0], ('some date', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minute_all(self):
        df = self.df
        result = df.cols.minute(cols='*')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [38, 25, 0, 59, 30, 0], ('some date', 'int64'): [59, 30, 25, 38, 0, 0], ('last date seen', 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'int64'): [0, 0, 12, 3, 38, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minute_multiple(self):
        df = self.df
        result = df.cols.minute(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [0, 0, 12, 3, 38, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minute_single(self):
        df = self.df
        result = df.cols.minute(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S -%Y')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [38, 25, 0, 59, 30, 0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_all(self):
        df = self.df
        result = df.cols.minutes_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_all_today(self):
        df = self.df
        result = df.cols.minutes_between(cols='*', round='up')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_multiple(self):
        df = self.df
        result = df.cols.minutes_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-2383200.0, -1811520.0, -5146560.0, -672480.0, -102240.0, 6206400.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-2383200.0, -1811520.0, -4209120.0, -692640.0, -102240.0, -4996800.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-1481760.0, -1219140.0, -1218372.0, -1219624.0, -4786179.0, -5002561.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_multiple_today(self):
        df = self.df
        result = df.cols.minutes_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [2620493.0, 3192173.0, -142867.0, 4331213.0, 4901453.0, 11210093.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [2620493.0, 3192173.0, 794573.0, 4311053.0, 4901453.0, 6893.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [3521933.0, 3784553.0, 3785321.0, 3784069.0, 217515.0, 1133.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_single(self):
        df = self.df
        result = df.cols.minutes_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S -%Y', round='down')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_minutes_between_single_timezone(self):
        df = self.df
        result = df.cols.minutes_between(cols=['date arrival', 'same date'], round='round')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_month_all(self):
        df = self.df
        result = df.cols.month(cols='*')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [9, 5, 8, 12, 1, 7], ('some date', 'int64'): [12, 1, 5, 9, 7, 8], ('last date seen', 'int64'): [10, 10, 12, 10, 10, 12], ('Date Type', 'int64'): [9, 8, 3, 6, 5, 8], ('timestamp', 'int64'): [12, 6, 6, 6, 4, 9]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_month_multiple(self):
        df = self.df
        result = df.cols.month(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [10, 10, 12, 10, 10, 12], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [9, 8, 3, 6, 5, 8], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [12, 6, 6, 6, 4, 9]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_month_single(self):
        df = self.df
        result = df.cols.month(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S -%Y')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [9, 5, 8, 12, 1, 7], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_all(self):
        df = self.df
        result = df.cols.months_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_all_today(self):
        df = self.df
        result = df.cols.months_between(cols='*', round='up')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_multiple(self):
        df = self.df
        result = df.cols.months_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_multiple_today(self):
        df = self.df
        result = df.cols.months_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_single(self):
        df = self.df
        result = df.cols.months_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S -%Y', round='down')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_months_between_single_timezone(self):
        df = self.df
        result = df.cols.months_between(cols=['date arrival', 'some date'], round='round')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_second_all(self):
        df = self.df
        result = df.cols.second(cols='*')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [11, 51, 1, 59, 19, 0], ('some date', 'int64'): [59, 19, 51, 11, 0, 1], ('last date seen', 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'int64'): [0, 0, 0, 59, 11, 1]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_second_multiple(self):
        df = self.df
        result = df.cols.second(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 0, 0, 0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [0, 0, 0, 59, 11, 1]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_second_single(self):
        df = self.df
        result = df.cols.second(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S -%Y')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [11, 51, 1, 59, 19, 0], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_all(self):
        df = self.df
        result = df.cols.seconds_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_all_today(self):
        df = self.df
        result = df.cols.seconds_between(cols='*', round='up')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_multiple(self):
        df = self.df
        result = df.cols.seconds_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [-142992000.0, -108691200.0, -308793600.0, -40348800.0, -6134400.0, 372384000.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [-142992000.0, -108691200.0, -252547200.0, -41558400.0, -6134400.0, -299808000.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [-88905600.0, -73148400.0, -73102320.0, -73177439.0, -287170691.0, -300153601.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_multiple_today(self):
        df = self.df
        result = df.cols.seconds_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('da', 'float64'): [157229567.0, 191530367.0, -8572033.0, 259872767.0, 294087167.0, 672605567.0], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('dt', 'float64'): [157229567.0, 191530367.0, 47674367.0, 258663167.0, 294087167.0, 413567.0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ('ts', 'float64'): [211315967.0, 227073167.0, 227119247.0, 227044128.0, 13050876.0, 67966.0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_single(self):
        df = self.df
        result = df.cols.seconds_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S -%Y', round='down')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_seconds_between_single_timezone(self):
        df = self.df
        result = df.cols.seconds_between(cols=['date arrival', 'same date'], round='round')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_weekday_all(self):
        df = self.df
        result = df.cols.weekday(cols='*')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [5, 5, 0, 4, 4, 1], ('some date', 'int64'): [4, 4, 5, 5, 1, 0], ('last date seen', 'int64'): [6, 3, 6, 6, 4, 1], ('Date Type', 'int64'): [5, 0, 6, 0, 3, 0], ('timestamp', 'int64'): [2, 1, 1, 1, 0, 4]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_weekday_multiple(self):
        df = self.df
        result = df.cols.weekday(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [6, 3, 6, 6, 4, 1], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [5, 0, 6, 0, 3, 0], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [2, 1, 1, 1, 0, 4]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_weekday_single(self):
        df = self.df
        result = df.cols.weekday(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S -%Y')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [5, 5, 0, 4, 4, 1], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_year_all(self):
        df = self.df
        result = df.cols.year(cols='*')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [2023, 2021, 2021, 1999, 2038, 1956], ('some date', 'int64'): [1999, 2038, 2021, 2023, 1956, 2021], ('last date seen', 'int64'): [2016, 2015, 2021, 2013, 2012, 2000], ('Date Type', 'int64'): [2016, 2015, 2020, 2013, 2012, 2021], ('timestamp', 'int64'): [2014, 2014, 2014, 2014, 2021, 2021]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_year_multiple(self):
        df = self.df
        result = df.cols.year(cols=['Date Type', 'timestamp', 'last date seen'], output_cols=['dt', 'ts', 'da'])
        expected = self.create_dataframe(dict={('date arrival', 'object'): ['Sat Sep 30 2023 11:38:11 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sun Aug 30 2021 00:00:01 -0400', 'Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Tue Jul 31 1956 12:00:00 -0400'], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ("last date seen_['dt', 'ts', 'da']", 'int64'): [2016, 2015, 2021, 2013, 2012, 2000], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ("Date Type_['dt', 'ts', 'da']", 'int64'): [2016, 2015, 2020, 2013, 2012, 2021], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')], ("timestamp_['dt', 'ts', 'da']", 'int64'): [2014, 2014, 2014, 2014, 2021, 2021]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_year_single(self):
        df = self.df
        result = df.cols.year(cols=['date arrival'], format='%a %b %d %Y %H:%M:%S -%Y')
        expected = self.create_dataframe(dict={('date arrival', 'int64'): [2023, 2021, 2021, 1999, 2038, 1956], ('some date', 'object'): ['Fri Dec 31 1999 23:59:59 -0400', 'Wednesday Jan 15 2038 10:30:19 -0400', 'Mon May 1 2021 13:25:51 -0400', 'Sat Sep 30 2023 11:38:11 -0400', 'Tue Jul 31 1956 12:00:00 -0400', 'Sun Aug 30 2021 00:00:01 -0400'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', '2000/05/12'], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2020-03-01 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2021-08-30 00:00:00')], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-12-24 00:00:00'), Timestamp('2014-06-24 15:00:00'), Timestamp('2014-06-24 02:12:00'), Timestamp('2014-06-24 23:03:59'), Timestamp('2021-04-05 17:38:11'), Timestamp('2021-09-03 00:00:01')]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_all(self):
        df = self.df
        result = df.cols.years_between(cols='*', value='2000-1-1', date_format='%Y-%d-%m', round=False)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_all_today(self):
        df = self.df
        result = df.cols.years_between(cols='*', round='up')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_multiple(self):
        df = self.df
        result = df.cols.years_between(cols=['Date Type', 'timestamp', 'last date seen'], value='29/02/2012', date_format='%d/%m/%Y', round='floor', output_cols=['dt', 'ts', 'da'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_multiple_today(self):
        df = self.df
        result = df.cols.years_between(cols=['Date Type', 'timestamp', 'last date seen'], round='ceil', output_cols=['dt', 'ts', 'da'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_single(self):
        df = self.df
        result = df.cols.years_between(cols=['date arrival'], value='Sat Sep 4 2021 10:05:39 -0400', date_format='%a %b %d %Y %H:%M:%S -%Y', round='down')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))

    def test_cols_years_between_single_timezone(self):
        df = self.df
        result = df.cols.years_between(cols=['date arrival', 'some date'], round='round')
        # The following value does not represent a correct output of the operation
        expected = self.dict
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
