from pyspark.sql.types import *

import optimus.helpers.functions_spark
from optimus import Optimus
import unittest
import numpy as np
nan = np.nan
op = Optimus(master='local')
source_df=op.create.df([('words', StringType(), True),('num', IntegerType(), True),('animals', StringType(), True),('thing', StringType(), True),('second', IntegerType(), True),('filter', StringType(), True)], [('  I like     fish  ', 1, 'dog dog', 'housé', 5, 'a'), ('    zombies', 2, 'cat', 'tv', 6, 'b'), ('simpsons   cat lady', 2, 'frog', 'table', 7, '1'), (None, 3, 'eagle', 'glass', 8, 'c')])
class Test_df_rows(unittest.TestCase):
	maxDiff = None
	@staticmethod
	def test_rows_append():
		actual_df = optimus.helpers.functions_spark.append([('this is a word', 2, 'this is an animal', 'this is a thing', 64, 'this is a filter')])
		expected_df = op.create.df([('words', StringType(), True),('num', IntegerType(), True),('animals', StringType(), True),('thing', StringType(), True),('second', IntegerType(), True),('filter', StringType(), True)], [('  I like     fish  ', 1, 'dog dog', 'housé', 5, 'a'), ('    zombies', 2, 'cat', 'tv', 6, 'b'), ('simpsons   cat lady', 2, 'frog', 'table', 7, '1'), (None, 3, 'eagle', 'glass', 8, 'c'), ('this is a word', 2, 'this is an animal', 'this is a thing', 64, 'this is a filter')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_rows_between():
		actual_df =source_df.rows.between('second',6,8)
		expected_df = op.create.df([('words', StringType(), True),('num', IntegerType(), True),('animals', StringType(), True),('thing', StringType(), True),('second', IntegerType(), True),('filter', StringType(), True)], [('simpsons   cat lady', 2, 'frog', 'table', 7, '1')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_rows_between_equal():
		actual_df =source_df.rows.between('second',6,8,equal=True)
		expected_df = op.create.df([('words', StringType(), True),('num', IntegerType(), True),('animals', StringType(), True),('thing', StringType(), True),('second', IntegerType(), True),('filter', StringType(), True)], [('    zombies', 2, 'cat', 'tv', 6, 'b'), ('simpsons   cat lady', 2, 'frog', 'table', 7, '1'), (None, 3, 'eagle', 'glass', 8, 'c')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_rows_between_invert_equal():
		actual_df =source_df.rows.between('second',6,8,invert=True,equal=True)
		expected_df = op.create.df([('words', StringType(), True),('num', IntegerType(), True),('animals', StringType(), True),('thing', StringType(), True),('second', IntegerType(), True),('filter', StringType(), True)], [('  I like     fish  ', 1, 'dog dog', 'housé', 5, 'a'), ('    zombies', 2, 'cat', 'tv', 6, 'b'), (None, 3, 'eagle', 'glass', 8, 'c')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_rows_drop_by_dtypes():
		actual_df =source_df.rows.drop_by_dtypes('filter','integer')
		expected_df = op.create.df([('words', StringType(), True),('num', IntegerType(), True),('animals', StringType(), True),('thing', StringType(), True),('second', IntegerType(), True),('filter', StringType(), True)], [('  I like     fish  ', 1, 'dog dog', 'housé', 5, 'a'), ('    zombies', 2, 'cat', 'tv', 6, 'b'), (None, 3, 'eagle', 'glass', 8, 'c')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_rows_is_in():
		actual_df =source_df.rows.is_in('num',2)
		expected_df = op.create.df([('words', StringType(), True),('num', IntegerType(), True),('animals', StringType(), True),('thing', StringType(), True),('second', IntegerType(), True),('filter', StringType(), True)], [('    zombies', 2, 'cat', 'tv', 6, 'b'), ('simpsons   cat lady', 2, 'frog', 'table', 7, '1')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_rows_select_by_dtypes():
		actual_df =source_df.rows.select_by_dtypes('filter','integer')
		expected_df = op.create.df([('words', StringType(), True),('num', IntegerType(), True),('animals', StringType(), True),('thing', StringType(), True),('second', IntegerType(), True),('filter', StringType(), True)], [('simpsons   cat lady', 2, 'frog', 'table', 7, '1')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_rows_sort():
		actual_df =source_df.rows.sort('num','desc')
		expected_df = op.create.df([('words', StringType(), True),('num', IntegerType(), True),('animals', StringType(), True),('thing', StringType(), True),('second', IntegerType(), True),('filter', StringType(), True)], [(None, 3, 'eagle', 'glass', 8, 'c'), ('    zombies', 2, 'cat', 'tv', 6, 'b'), ('simpsons   cat lady', 2, 'frog', 'table', 7, '1'), ('  I like     fish  ', 1, 'dog dog', 'housé', 5, 'a')])
		assert (expected_df.collect() == actual_df.collect())
