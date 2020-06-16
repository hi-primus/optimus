from pyspark.sql.types import *
from optimus import Optimus
from optimus.helpers.json import json_enconding
from optimus.helpers.functions import deep_sort
import unittest
import numpy as np
nan = np.nan
import datetime
op = Optimus(master='local')
source_df=op.create.df({"names":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"height(ft)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"function":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"rank":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"age":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"weight(t)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"japanese name":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last position seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"date arrival":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last date seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"attributes":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"Date Type":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"timestamp":{"alignment":8,"byteorder":"=","descr":[["","<M8[ns]"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"M","name":"datetime64[ns]","ndim":0,"num":21,"str":"<M8[ns]"},"Cybertronian":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"function(binary)":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"NullType":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"}}, {'sample': {'columns': [{'title': 'names'}, {'title': 'height(ft)'}, {'title': 'function'}, {'title': 'rank'}, {'title': 'age'}, {'title': 'weight(t)'}, {'title': 'japanese name'}, {'title': 'last position seen'}, {'title': 'date arrival'}, {'title': 'last date seen'}, {'title': 'attributes'}, {'title': 'Date Type'}, {'title': 'timestamp'}, {'title': 'Cybertronian'}, {'title': 'function(binary)'}, {'title': 'NullType'}], 'value': [["Optim'us", -28.0, 'Leader', 10.0, 5000000.0, 4.3, ['Inochi', 'Convoy'], '19.442735,-99.201111', '1980/04/10', '2016/09/10', [8.5344, 4300.0], datetime.date(2016, 9, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Leader'), None], ['bumbl#ebéé  ', 17.0, 'Espionage', 7.0, 5000000.0, 2.0, ['Bumble', 'Goldback'], '10.642707,-71.612534', '1980/04/10', '2015/08/10', [5.334, 2000.0], datetime.date(2015, 8, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Espionage'), None], ['ironhide&', 26.0, 'Security', 7.0, 5000000.0, 4.0, ['Roadbuster'], '37.789563,-122.400356', '1980/04/10', '2014/07/10', [7.9248, 4000.0], datetime.date(2014, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Security'), None], ['Jazz', 13.0, 'First Lieutenant', 8.0, 5000000.0, 1.8, ['Meister'], '33.670666,-117.841553', '1980/04/10', '2013/06/10', [3.9624, 1800.0], datetime.date(2013, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'First Lieutenant'), None], ['Megatron', nan, 'None', 10.0, 5000000.0, 5.7, ['Megatron'], None, '1980/04/10', '2012/05/10', [None, 5700.0], datetime.date(2012, 5, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'None'), None], ['Metroplex_)^$', 300.0, 'Battle Station', 8.0, 5000000.0, nan, ['Metroflex'], None, '1980/04/10', '2011/04/10', [91.44, None], datetime.date(2011, 4, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Battle Station'), None], [None, nan, None, nan, nan, nan, None, None, None, None, None, None, NaT, None, None, None]]}})
class Test_df_cols_dask(unittest.TestCase):
	maxDiff = None
	@staticmethod
	def test_cols_abs():
		actual_df =source_df.cols.abs('weight(t)')
		expected_df = op.create.df({"names":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"height(ft)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"function":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"rank":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"age":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"weight(t)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"japanese name":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last position seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"date arrival":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last date seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"attributes":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"Date Type":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"timestamp":{"alignment":8,"byteorder":"=","descr":[["","<M8[ns]"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"M","name":"datetime64[ns]","ndim":0,"num":21,"str":"<M8[ns]"},"Cybertronian":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"function(binary)":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"NullType":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"}}, {'sample': {'columns': [{'title': 'names'}, {'title': 'height(ft)'}, {'title': 'function'}, {'title': 'rank'}, {'title': 'age'}, {'title': 'weight(t)'}, {'title': 'japanese name'}, {'title': 'last position seen'}, {'title': 'date arrival'}, {'title': 'last date seen'}, {'title': 'attributes'}, {'title': 'Date Type'}, {'title': 'timestamp'}, {'title': 'Cybertronian'}, {'title': 'function(binary)'}, {'title': 'NullType'}], 'value': [["Optim'us", -28.0, 'Leader', 10.0, 5000000.0, 4.3, ['Inochi', 'Convoy'], '19.442735,-99.201111', '1980/04/10', '2016/09/10', [8.5344, 4300.0], datetime.date(2016, 9, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Leader'), None], ['bumbl#ebéé  ', 17.0, 'Espionage', 7.0, 5000000.0, 2.0, ['Bumble', 'Goldback'], '10.642707,-71.612534', '1980/04/10', '2015/08/10', [5.334, 2000.0], datetime.date(2015, 8, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Espionage'), None], ['ironhide&', 26.0, 'Security', 7.0, 5000000.0, 4.0, ['Roadbuster'], '37.789563,-122.400356', '1980/04/10', '2014/07/10', [7.9248, 4000.0], datetime.date(2014, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Security'), None], ['Jazz', 13.0, 'First Lieutenant', 8.0, 5000000.0, 1.8, ['Meister'], '33.670666,-117.841553', '1980/04/10', '2013/06/10', [3.9624, 1800.0], datetime.date(2013, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'First Lieutenant'), None], ['Megatron', nan, 'None', 10.0, 5000000.0, 5.7, ['Megatron'], None, '1980/04/10', '2012/05/10', [None, 5700.0], datetime.date(2012, 5, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'None'), None], ['Metroplex_)^$', 300.0, 'Battle Station', 8.0, 5000000.0, nan, ['Metroflex'], None, '1980/04/10', '2011/04/10', [91.44, None], datetime.date(2011, 4, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Battle Station'), None], [None, nan, None, nan, nan, nan, None, None, None, None, None, None, NaT, None, None, None]]}})
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_cols_abs_all_columns():
		actual_df =source_df.cols.abs('*')
		expected_df = op.create.df({"names":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"height(ft)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"function":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"rank":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"age":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"weight(t)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"japanese name":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last position seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"date arrival":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last date seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"attributes":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"Date Type":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"timestamp":{"alignment":8,"byteorder":"=","descr":[["","<M8[ns]"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"M","name":"datetime64[ns]","ndim":0,"num":21,"str":"<M8[ns]"},"Cybertronian":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"function(binary)":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"NullType":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"}}, {'sample': {'columns': [{'title': 'names'}, {'title': 'height(ft)'}, {'title': 'function'}, {'title': 'rank'}, {'title': 'age'}, {'title': 'weight(t)'}, {'title': 'japanese name'}, {'title': 'last position seen'}, {'title': 'date arrival'}, {'title': 'last date seen'}, {'title': 'attributes'}, {'title': 'Date Type'}, {'title': 'timestamp'}, {'title': 'Cybertronian'}, {'title': 'function(binary)'}, {'title': 'NullType'}], 'value': [["Optim'us", -28.0, 'Leader', 10.0, 5000000.0, 4.3, ['Inochi', 'Convoy'], '19.442735,-99.201111', '1980/04/10', '2016/09/10', [8.5344, 4300.0], datetime.date(2016, 9, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Leader'), None], ['bumbl#ebéé  ', 17.0, 'Espionage', 7.0, 5000000.0, 2.0, ['Bumble', 'Goldback'], '10.642707,-71.612534', '1980/04/10', '2015/08/10', [5.334, 2000.0], datetime.date(2015, 8, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Espionage'), None], ['ironhide&', 26.0, 'Security', 7.0, 5000000.0, 4.0, ['Roadbuster'], '37.789563,-122.400356', '1980/04/10', '2014/07/10', [7.9248, 4000.0], datetime.date(2014, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Security'), None], ['Jazz', 13.0, 'First Lieutenant', 8.0, 5000000.0, 1.8, ['Meister'], '33.670666,-117.841553', '1980/04/10', '2013/06/10', [3.9624, 1800.0], datetime.date(2013, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'First Lieutenant'), None], ['Megatron', nan, 'None', 10.0, 5000000.0, 5.7, ['Megatron'], None, '1980/04/10', '2012/05/10', [None, 5700.0], datetime.date(2012, 5, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'None'), None], ['Metroplex_)^$', 300.0, 'Battle Station', 8.0, 5000000.0, nan, ['Metroflex'], None, '1980/04/10', '2011/04/10', [91.44, None], datetime.date(2011, 4, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Battle Station'), None], [None, nan, None, nan, nan, nan, None, None, None, None, None, None, NaT, None, None, None]]}})
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_cols_count():
		actual_df =source_df.cols.count()
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding(16)
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_count_na():
		actual_df =source_df.cols.count_na('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'count_na': {'height(ft)': 2}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_count_na_all_columns():
		actual_df =source_df.cols.count_na('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'count_na': {'names': 1, 'height(ft)': 2, 'function': 1, 'rank': 1, 'age': 1, 'weight(t)': 2, 'japanese name': 1, 'last position seen': 3, 'date arrival': 1, 'last date seen': 1, 'attributes': 1, 'Date Type': 1, 'timestamp': 1, 'Cybertronian': 1, 'function(binary)': 1, 'NullType': 7}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_count_uniques():
		actual_df =source_df.cols.count_uniques('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'height(ft)': {'count_uniques': 5}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_count_zeros():
		actual_df =source_df.cols.count_zeros('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'zeros': {'height(ft)': 0}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_count_zeros_all_columns():
		actual_df =source_df.cols.count_zeros('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'zeros': {'names': 0, 'height(ft)': 0, 'function': 0, 'rank': 0, 'age': 0, 'weight(t)': 0, 'japanese name': 0, 'last position seen': 0, 'date arrival': 0, 'last date seen': 0, 'attributes': 0, 'Date Type': 0, 'Cybertronian': 0, 'function(binary)': 0, 'NullType': 0}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_kurt():
		actual_df =source_df.cols.kurt('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'kurtosis': {'height(ft)': nan}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_kurt_all_columns():
		actual_df =source_df.cols.kurt('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'kurtosis': {'height(ft)': nan, 'rank': nan, 'age': nan, 'weight(t)': nan}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_mad():
		actual_df =source_df.cols.mad('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'height(ft)': {'mad': 9.0}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_mad_all_columns():
		actual_df =source_df.cols.mad('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'height(ft)': {'mad': 9.0}, 'rank': {'mad': 1.0}, 'age': {'mad': 0.0}, 'weight(t)': {'mad': 1.7000000000000002}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_max():
		actual_df =source_df.cols.max('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'height(ft)': 300.0})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_max_all_columns():
		actual_df =source_df.cols.max('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'Cybertronian': True, 'NullType': nan, 'age': 5000000.0, 'height(ft)': 300.0, 'rank': 10.0, 'timestamp': Timestamp('2014-06-24 00:00:00'), 'weight(t)': 5.7})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_mean():
		actual_df =source_df.cols.mean('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'mean': {'height(ft)': 65.6}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_mean_all_columns():
		actual_df =source_df.cols.mean('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'mean': {'height(ft)': 65.6, 'rank': 8.333333333333334, 'age': 5000000.0, 'weight(t)': 3.56}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_median():
		actual_df =source_df.cols.median('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding(17.0)
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_median_all_columns():
		actual_df =source_df.cols.median('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'height(ft)': 17.0, 'rank': 8.0, 'age': 5000000.0, 'weight(t)': 4.0})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_min():
		actual_df =source_df.cols.min('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'height(ft)': -28.0})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_min_all_columns():
		actual_df =source_df.cols.min('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'Cybertronian': True, 'NullType': nan, 'age': 5000000.0, 'height(ft)': -28.0, 'rank': 7.0, 'timestamp': Timestamp('2014-06-24 00:00:00'), 'weight(t)': 1.8})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_mode():
		actual_df =source_df.cols.mode('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'mode': {'height(ft)': [-28.0, 13.0, 17.0, 26.0, 300.0]}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_mode_all_columns():
		actual_df =source_df.cols.mode('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding(Delayed('_mode-231bcbb5-6e44-4aa6-a3b1-77b3678dc034'))
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_percentile():
		actual_df =source_df.cols.percentile('height(ft)',[0.05,0.25],1)
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'percentile': {0.05: -19.8, 0.25: 13.0}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_percentile_all_columns():
		actual_df =source_df.cols.percentile('*',[0.05,0.25],1)
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'percentile': {'height(ft)': {0.05: -19.8, 0.25: 13.0}, 'rank': {0.05: 7.0, 0.25: 7.25}, 'age': {0.05: 5000000.0, 0.25: 5000000.0}, 'weight(t)': {0.05: 1.8400000000000003, 0.25: 2.0}}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_range():
		actual_df =source_df.cols.range('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'range': {'height(ft)': {'min': -28.0, 'max': 300.0}}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_range_all_columns():
		actual_df =source_df.cols.range('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'range': {None}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_remove():
		actual_df =source_df.cols.remove('function','i')
		expected_df = op.create.df({"names":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"height(ft)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"function":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"rank":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"age":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"weight(t)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"japanese name":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last position seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"date arrival":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last date seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"attributes":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"Date Type":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"timestamp":{"alignment":8,"byteorder":"=","descr":[["","<M8[ns]"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"M","name":"datetime64[ns]","ndim":0,"num":21,"str":"<M8[ns]"},"Cybertronian":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"function(binary)":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"NullType":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"}}, {'sample': {'columns': [{'title': 'names'}, {'title': 'height(ft)'}, {'title': 'function'}, {'title': 'rank'}, {'title': 'age'}, {'title': 'weight(t)'}, {'title': 'japanese name'}, {'title': 'last position seen'}, {'title': 'date arrival'}, {'title': 'last date seen'}, {'title': 'attributes'}, {'title': 'Date Type'}, {'title': 'timestamp'}, {'title': 'Cybertronian'}, {'title': 'function(binary)'}, {'title': 'NullType'}], 'value': [["Optim'us", -28.0, 'Leader', 10.0, 5000000.0, 4.3, ['Inochi', 'Convoy'], '19.442735,-99.201111', '1980/04/10', '2016/09/10', [8.5344, 4300.0], datetime.date(2016, 9, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Leader'), None], ['bumbl#ebéé  ', 17.0, 'Esponage', 7.0, 5000000.0, 2.0, ['Bumble', 'Goldback'], '10.642707,-71.612534', '1980/04/10', '2015/08/10', [5.334, 2000.0], datetime.date(2015, 8, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Espionage'), None], ['ironhide&', 26.0, 'Securty', 7.0, 5000000.0, 4.0, ['Roadbuster'], '37.789563,-122.400356', '1980/04/10', '2014/07/10', [7.9248, 4000.0], datetime.date(2014, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Security'), None], ['Jazz', 13.0, 'Frst Leutenant', 8.0, 5000000.0, 1.8, ['Meister'], '33.670666,-117.841553', '1980/04/10', '2013/06/10', [3.9624, 1800.0], datetime.date(2013, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'First Lieutenant'), None], ['Megatron', nan, 'None', 10.0, 5000000.0, 5.7, ['Megatron'], None, '1980/04/10', '2012/05/10', [None, 5700.0], datetime.date(2012, 5, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'None'), None], ['Metroplex_)^$', 300.0, 'Battle Staton', 8.0, 5000000.0, nan, ['Metroflex'], None, '1980/04/10', '2011/04/10', [91.44, None], datetime.date(2011, 4, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Battle Station'), None], [None, nan, 'None', nan, nan, nan, None, None, None, None, None, None, NaT, None, None, None]]}})
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_cols_remove_list():
		actual_df =source_df.cols.remove('function',['a','i','Es'])
		expected_df = op.create.df({"names":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"height(ft)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"function":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"rank":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"age":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"weight(t)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"japanese name":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last position seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"date arrival":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last date seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"attributes":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"Date Type":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"timestamp":{"alignment":8,"byteorder":"=","descr":[["","<M8[ns]"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"M","name":"datetime64[ns]","ndim":0,"num":21,"str":"<M8[ns]"},"Cybertronian":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"function(binary)":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"NullType":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"}}, {'sample': {'columns': [{'title': 'names'}, {'title': 'height(ft)'}, {'title': 'function'}, {'title': 'rank'}, {'title': 'age'}, {'title': 'weight(t)'}, {'title': 'japanese name'}, {'title': 'last position seen'}, {'title': 'date arrival'}, {'title': 'last date seen'}, {'title': 'attributes'}, {'title': 'Date Type'}, {'title': 'timestamp'}, {'title': 'Cybertronian'}, {'title': 'function(binary)'}, {'title': 'NullType'}], 'value': [["Optim'us", -28.0, 'Leder', 10.0, 5000000.0, 4.3, ['Inochi', 'Convoy'], '19.442735,-99.201111', '1980/04/10', '2016/09/10', [8.5344, 4300.0], datetime.date(2016, 9, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Leader'), None], ['bumbl#ebéé  ', 17.0, 'ponge', 7.0, 5000000.0, 2.0, ['Bumble', 'Goldback'], '10.642707,-71.612534', '1980/04/10', '2015/08/10', [5.334, 2000.0], datetime.date(2015, 8, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Espionage'), None], ['ironhide&', 26.0, 'Securty', 7.0, 5000000.0, 4.0, ['Roadbuster'], '37.789563,-122.400356', '1980/04/10', '2014/07/10', [7.9248, 4000.0], datetime.date(2014, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Security'), None], ['Jazz', 13.0, 'Frst Leutennt', 8.0, 5000000.0, 1.8, ['Meister'], '33.670666,-117.841553', '1980/04/10', '2013/06/10', [3.9624, 1800.0], datetime.date(2013, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'First Lieutenant'), None], ['Megatron', nan, 'None', 10.0, 5000000.0, 5.7, ['Megatron'], None, '1980/04/10', '2012/05/10', [None, 5700.0], datetime.date(2012, 5, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'None'), None], ['Metroplex_)^$', 300.0, 'Bttle Stton', 8.0, 5000000.0, nan, ['Metroflex'], None, '1980/04/10', '2011/04/10', [91.44, None], datetime.date(2011, 4, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Battle Station'), None], [None, nan, 'None', nan, nan, nan, None, None, None, None, None, None, NaT, None, None, None]]}})
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_cols_remove_list_output():
		actual_df =source_df.cols.remove('function',['a','i','Es'],output_cols='function_new')
		expected_df = op.create.df({"names":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"height(ft)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"function":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"function_new":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"rank":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"age":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"weight(t)":{"alignment":8,"byteorder":"=","descr":[["","<f8"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"f","name":"float64","ndim":0,"num":12,"str":"<f8"},"japanese name":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last position seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"date arrival":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"last date seen":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"attributes":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"Date Type":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"timestamp":{"alignment":8,"byteorder":"=","descr":[["","<M8[ns]"]],"flags":0,"isalignedstruct":false,"isnative":true,"kind":"M","name":"datetime64[ns]","ndim":0,"num":21,"str":"<M8[ns]"},"Cybertronian":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"function(binary)":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"},"NullType":{"alignment":8,"byteorder":"|","descr":[["","|O"]],"flags":63,"isalignedstruct":false,"isnative":true,"kind":"O","name":"object","ndim":0,"num":17,"str":"|O"}}, {'sample': {'columns': [{'title': 'names'}, {'title': 'height(ft)'}, {'title': 'function'}, {'title': 'function_new'}, {'title': 'rank'}, {'title': 'age'}, {'title': 'weight(t)'}, {'title': 'japanese name'}, {'title': 'last position seen'}, {'title': 'date arrival'}, {'title': 'last date seen'}, {'title': 'attributes'}, {'title': 'Date Type'}, {'title': 'timestamp'}, {'title': 'Cybertronian'}, {'title': 'function(binary)'}, {'title': 'NullType'}], 'value': [["Optim'us", -28.0, 'Leader', 'Leder', 10.0, 5000000.0, 4.3, ['Inochi', 'Convoy'], '19.442735,-99.201111', '1980/04/10', '2016/09/10', [8.5344, 4300.0], datetime.date(2016, 9, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Leader'), None], ['bumbl#ebéé  ', 17.0, 'Espionage', 'ponge', 7.0, 5000000.0, 2.0, ['Bumble', 'Goldback'], '10.642707,-71.612534', '1980/04/10', '2015/08/10', [5.334, 2000.0], datetime.date(2015, 8, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Espionage'), None], ['ironhide&', 26.0, 'Security', 'Securty', 7.0, 5000000.0, 4.0, ['Roadbuster'], '37.789563,-122.400356', '1980/04/10', '2014/07/10', [7.9248, 4000.0], datetime.date(2014, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Security'), None], ['Jazz', 13.0, 'First Lieutenant', 'Frst Leutennt', 8.0, 5000000.0, 1.8, ['Meister'], '33.670666,-117.841553', '1980/04/10', '2013/06/10', [3.9624, 1800.0], datetime.date(2013, 6, 24), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'First Lieutenant'), None], ['Megatron', nan, 'None', 'None', 10.0, 5000000.0, 5.7, ['Megatron'], None, '1980/04/10', '2012/05/10', [None, 5700.0], datetime.date(2012, 5, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'None'), None], ['Metroplex_)^$', 300.0, 'Battle Station', 'Bttle Stton', 8.0, 5000000.0, nan, ['Metroflex'], None, '1980/04/10', '2011/04/10', [91.44, None], datetime.date(2011, 4, 10), Timestamp('2014-06-24 00:00:00'), True, bytearray(b'Battle Station'), None], [None, nan, None, 'None', nan, nan, nan, None, None, None, None, None, None, NaT, None, None, None]]}})
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_cols_skewness():
		actual_df =source_df.cols.skewness('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'skewness': {'height(ft)': nan}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_skewness_all_columns():
		actual_df =source_df.cols.skewness('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'skewness': {'height(ft)': nan, 'rank': nan, 'age': nan, 'weight(t)': nan}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_std():
		actual_df =source_df.cols.std('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'stddev': {'height(ft)': 132.66612227694}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_std_all_columns():
		actual_df =source_df.cols.std('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'stddev': {'height(ft)': 132.66612227694, 'rank': 1.3662601021279464, 'age': 0.0, 'weight(t)': 1.6471186963907611}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_sum():
		actual_df =source_df.cols.sum('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding(328.0)
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_sum_all_columns():
		actual_df =source_df.cols.sum('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'height(ft)': 328.0, 'rank': 50.0, 'age': 30000000.0, 'weight(t)': 17.8})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_unique():
		actual_df =source_df.cols.unique('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding(0    -28.0
1     17.0
2     26.0
3     13.0
4      NaN
5    300.0
Name: height(ft), dtype: float64)
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_unique_all_columns():
		actual_df =source_df.cols.unique('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding(Dask DataFrame Structure:
                names height(ft) function     rank      age weight(t) japanese name last position seen date arrival last date seen attributes Date Type       timestamp Cybertronian function(binary) NullType
npartitions=1                                                                                                                                                                                                 
               object    float64   object  float64  float64   float64        object             object       object         object     object    object  datetime64[ns]       object           object   object
                  ...        ...      ...      ...      ...       ...           ...                ...          ...            ...        ...       ...             ...          ...              ...      ...
Dask Name: drop-duplicates-agg, 3 tasks)
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_value_counts():
		actual_df =source_df.cols.value_counts('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'height(ft)': {300.0: 1, 26.0: 1, 17.0: 1, 13.0: 1, -28.0: 1}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_value_counts_all_columns():
		actual_df =source_df.cols.value_counts('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'names': {'ironhide&': 1, 'bumbl#ebéé  ': 1, "Optim'us": 1, 'Metroplex_)^$': 1, 'Megatron': 1, 'Jazz': 1}, 'height(ft)': {300.0: 1, 26.0: 1, 17.0: 1, 13.0: 1, -28.0: 1}, 'function': {'Security': 1, 'None': 1, 'Leader': 1, 'First Lieutenant': 1, 'Espionage': 1, 'Battle Station': 1}, 'rank': {10.0: 2, 8.0: 2, 7.0: 2}, 'age': {5000000.0: 6}, 'weight(t)': {5.7: 1, 4.3: 1, 4.0: 1, 2.0: 1, 1.8: 1}, 'last position seen': {'37.789563,-122.400356': 1, '33.670666,-117.841553': 1, '19.442735,-99.201111': 1, '10.642707,-71.612534': 1}, 'date arrival': {'1980/04/10': 6}, 'last date seen': {'2016/09/10': 1, '2015/08/10': 1, '2014/07/10': 1, '2013/06/10': 1, '2012/05/10': 1, '2011/04/10': 1}, 'Date Type': {datetime.date(2016, 9, 10): 1, datetime.date(2015, 8, 10): 1, datetime.date(2014, 6, 24): 1, datetime.date(2013, 6, 24): 1, datetime.date(2012, 5, 10): 1, datetime.date(2011, 4, 10): 1}, 'timestamp': {Timestamp('2014-06-24 00:00:00'): 6}, 'Cybertronian': {True: 6}, 'NullType': {}})
		assert(expected_value == actual_df)
ry)': None, 'NullType': {}})
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_variance():
		actual_df =source_df.cols.variance('height(ft)')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding(17600.3)
		assert(expected_value == actual_df)
	@staticmethod
	def test_cols_variance_all_columns():
		actual_df =source_df.cols.variance('*')
		actual_df =json_enconding(actual_df)
		expected_value =json_enconding({'height(ft)': 17600.3, 'rank': 1.8666666666666665, 'age': 0.0, 'weight(t)': 2.713})
		assert(expected_value == actual_df)
