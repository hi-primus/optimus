from pyspark.sql.types import *
from optimus import Optimus
from optimus.helpers.functions import json_enconding 
from pyspark.ml.linalg import Vectors, VectorUDT, DenseVector
import numpy as np
nan = np.nan
import datetime
from pyspark.sql import functions as F
op = Optimus(master='local')
source_df=op.create.df([('id', IntegerType(), True),('firstName', StringType(), True),('lastName', StringType(), True),('billingId', IntegerType(), True),('product', StringType(), True),('price', IntegerType(), True),('birth', StringType(), True),('dummyCol', StringType(), True)], [(1, 'Luis', 'Alvarez$$%!', 123, 'Cake', 10, '1980/07/07', 'never'), (2, 'André', 'Ampère', 423, 'piza', 8, '1950/07/08', 'gonna'), (3, 'NiELS', 'Böhr//((%%', 551, 'pizza', 8, '1990/07/09', 'give'), (4, 'PAUL', 'dirac$', 521, 'pizza', 8, '1954/07/10', 'you'), (5, 'Albert', 'Einstein', 634, 'pizza', 8, '1990/07/11', 'up'), (6, 'Galileo', '             GALiLEI', 672, 'arepa', 5, '1930/08/12', 'never'), (7, 'CaRL', 'Ga%%%uss', 323, 'taco', 3, '1970/07/13', 'gonna'), (8, 'David', 'H$$$ilbert', 624, 'taaaccoo', 3, '1950/07/14', 'let'), (9, 'Johannes', 'KEPLER', 735, 'taco', 3, '1920/04/22', 'you'), (10, 'JaMES', 'M$$ax%%well', 875, 'taco', 3, '1923/03/12', 'down'), (11, 'Isaac', 'Newton', 992, 'pasta', 9, '1999/02/15', 'never '), (12, 'Emmy%%', 'Nöether$', 234, 'pasta', 9, '1993/12/08', 'gonna'), (13, 'Max!!!', 'Planck!!!', 111, 'hamburguer', 4, '1994/01/04', 'run '), (14, 'Fred', 'Hoy&&&le', 553, 'pizzza', 8, '1997/06/27', 'around'), (15, '(((   Heinrich )))))', 'Hertz', 116, 'pizza', 8, '1956/11/30', 'and'), (16, 'William', 'Gilbert###', 886, 'BEER', 2, '1958/03/26', 'desert'), (17, 'Marie', 'CURIE', 912, 'Rice', 1, '2000/03/22', 'you'), (18, 'Arthur', 'COM%%%pton', 812, '110790', 5, '1899/01/01', '#'), (19, 'JAMES', 'Chadwick', 467, 'null', 10, '1921/05/03', '#')])
class Testop_io(object):
	@staticmethod
	def test_load_csv():
		actual_df =op.load.csv('../../examples/data/foo.csv')
		expected_df = op.create.df([('id', IntegerType(), True),('firstName', StringType(), True),('lastName', StringType(), True),('billingId', IntegerType(), True),('product', StringType(), True),('price', IntegerType(), True),('birth', StringType(), True),('dummyCol', StringType(), True)], [(1, 'Luis', 'Alvarez$$%!', 123, 'Cake', 10, '1980/07/07', 'never'), (2, 'André', 'Ampère', 423, 'piza', 8, '1950/07/08', 'gonna'), (3, 'NiELS', 'Böhr//((%%', 551, 'pizza', 8, '1990/07/09', 'give'), (4, 'PAUL', 'dirac$', 521, 'pizza', 8, '1954/07/10', 'you'), (5, 'Albert', 'Einstein', 634, 'pizza', 8, '1990/07/11', 'up'), (6, 'Galileo', '             GALiLEI', 672, 'arepa', 5, '1930/08/12', 'never'), (7, 'CaRL', 'Ga%%%uss', 323, 'taco', 3, '1970/07/13', 'gonna'), (8, 'David', 'H$$$ilbert', 624, 'taaaccoo', 3, '1950/07/14', 'let'), (9, 'Johannes', 'KEPLER', 735, 'taco', 3, '1920/04/22', 'you'), (10, 'JaMES', 'M$$ax%%well', 875, 'taco', 3, '1923/03/12', 'down'), (11, 'Isaac', 'Newton', 992, 'pasta', 9, '1999/02/15', 'never '), (12, 'Emmy%%', 'Nöether$', 234, 'pasta', 9, '1993/12/08', 'gonna'), (13, 'Max!!!', 'Planck!!!', 111, 'hamburguer', 4, '1994/01/04', 'run '), (14, 'Fred', 'Hoy&&&le', 553, 'pizzza', 8, '1997/06/27', 'around'), (15, '(((   Heinrich )))))', 'Hertz', 116, 'pizza', 8, '1956/11/30', 'and'), (16, 'William', 'Gilbert###', 886, 'BEER', 2, '1958/03/26', 'desert'), (17, 'Marie', 'CURIE', 912, 'Rice', 1, '2000/03/22', 'you'), (18, 'Arthur', 'COM%%%pton', 812, '110790', 5, '1899/01/01', '#'), (19, 'JAMES', 'Chadwick', 467, 'null', 10, '1921/05/03', '#')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_load_json():
		actual_df =op.load.json('../../examples/data/foo.json')
		expected_df = op.create.df([('billingId', LongType(), True),('birth', StringType(), True),('dummyCol', StringType(), True),('firstName', StringType(), True),('id', LongType(), True),('lastName', StringType(), True),('price', LongType(), True),('product', StringType(), True)], [(123, '1980/07/07', 'never', 'Luis', 1, 'Alvarez$$%!', 10, 'Cake')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_load_parquet():
		actual_df =op.load.parquet('../../examples/data/foo.parquet')
		expected_df = op.create.df([('id', IntegerType(), True),('firstName', StringType(), True),('lastName', StringType(), True),('billingId', IntegerType(), True),('product', StringType(), True),('price', IntegerType(), True),('birth', StringType(), True),('dummyCol', StringType(), True)], [(1, 'Luis', 'Alvarez$$%!', 123, 'Cake', 10, '1980/07/07', 'never'), (2, 'André', 'Ampère', 423, 'piza', 8, '1950/07/08', 'gonna'), (3, 'NiELS', 'Böhr//((%%', 551, 'pizza', 8, '1990/07/09', 'give'), (4, 'PAUL', 'dirac$', 521, 'pizza', 8, '1954/07/10', 'you'), (5, 'Albert', 'Einstein', 634, 'pizza', 8, '1990/07/11', 'up'), (6, 'Galileo', '             GALiLEI', 672, 'arepa', 5, '1930/08/12', 'never'), (7, 'CaRL', 'Ga%%%uss', 323, 'taco', 3, '1970/07/13', 'gonna'), (8, 'David', 'H$$$ilbert', 624, 'taaaccoo', 3, '1950/07/14', 'let'), (9, 'Johannes', 'KEPLER', 735, 'taco', 3, '1920/04/22', 'you'), (10, 'JaMES', 'M$$ax%%well', 875, 'taco', 3, '1923/03/12', 'down'), (11, 'Isaac', 'Newton', 992, 'pasta', 9, '1999/02/15', 'never '), (12, 'Emmy%%', 'Nöether$', 234, 'pasta', 9, '1993/12/08', 'gonna'), (13, 'Max!!!', 'Planck!!!', 111, 'hamburguer', 4, '1994/01/04', 'run '), (14, 'Fred', 'Hoy&&&le', 553, 'pizzza', 8, '1997/06/27', 'around'), (15, '(((   Heinrich )))))', 'Hertz', 116, 'pizza', 8, '1956/11/30', 'and'), (16, 'William', 'Gilbert###', 886, 'BEER', 2, '1958/03/26', 'desert'), (17, 'Marie', 'CURIE', 912, 'Rice', 1, '2000/03/22', 'you'), (18, 'Arthur', 'COM%%%pton', 812, '110790', 5, '1899/01/01', '#'), (19, 'JAMES', 'Chadwick', 467, 'null', 10, '1921/05/03', '#')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_load_url_csv():
		actual_df =op.load.url('https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.csv')
		expected_df = op.create.df([('id', IntegerType(), True),('firstName', StringType(), True),('lastName', StringType(), True),('billingId', IntegerType(), True),('product', StringType(), True),('price', IntegerType(), True),('birth', StringType(), True),('dummyCol', StringType(), True)], [(1, 'Luis', 'Alvarez$$%!', 123, 'Cake', 10, '1980/07/07', 'never'), (2, 'André', 'Ampère', 423, 'piza', 8, '1950/07/08', 'gonna'), (3, 'NiELS', 'Böhr//((%%', 551, 'pizza', 8, '1990/07/09', 'give'), (4, 'PAUL', 'dirac$', 521, 'pizza', 8, '1954/07/10', 'you'), (5, 'Albert', 'Einstein', 634, 'pizza', 8, '1990/07/11', 'up'), (6, 'Galileo', '             GALiLEI', 672, 'arepa', 5, '1930/08/12', 'never'), (7, 'CaRL', 'Ga%%%uss', 323, 'taco', 3, '1970/07/13', 'gonna'), (8, 'David', 'H$$$ilbert', 624, 'taaaccoo', 3, '1950/07/14', 'let'), (9, 'Johannes', 'KEPLER', 735, 'taco', 3, '1920/04/22', 'you'), (10, 'JaMES', 'M$$ax%%well', 875, 'taco', 3, '1923/03/12', 'down'), (11, 'Isaac', 'Newton', 992, 'pasta', 9, '1999/02/15', 'never '), (12, 'Emmy%%', 'Nöether$', 234, 'pasta', 9, '1993/12/08', 'gonna'), (13, 'Max!!!', 'Planck!!!', 111, 'hamburguer', 4, '1994/01/04', 'run '), (14, 'Fred', 'Hoy&&&le', 553, 'pizzza', 8, '1997/06/27', 'around'), (15, '(((   Heinrich )))))', 'Hertz', 116, 'pizza', 8, '1956/11/30', 'and'), (16, 'William', 'Gilbert###', 886, 'BEER', 2, '1958/03/26', 'desert'), (17, 'Marie', 'CURIE', 912, 'Rice', 1, '2000/03/22', 'you'), (18, 'Arthur', 'COM%%%pton', 812, '110790', 5, '1899/01/01', '#'), (19, 'JAMES', 'Chadwick', 467, 'null', 10, '1921/05/03', '#')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_load_url_json():
		actual_df =op.load.url('https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.json','json')
		expected_df = op.create.df([('billingId', LongType(), True),('birth', StringType(), True),('dummyCol', StringType(), True),('firstName', StringType(), True),('id', LongType(), True),('lastName', StringType(), True),('price', LongType(), True),('product', StringType(), True)], [(123, '1980/07/07', 'never', 'Luis', 1, 'Alvarez$$%!', 10, 'Cake')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_load_url_parquet():
		actual_df =op.load.url('https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.parquet','parquet')
		expected_df = op.create.df([('id', IntegerType(), True),('firstName', StringType(), True),('lastName', StringType(), True),('billingId', IntegerType(), True),('product', StringType(), True),('price', IntegerType(), True),('birth', StringType(), True),('dummyCol', StringType(), True)], [(1, 'Luis', 'Alvarez$$%!', 123, 'Cake', 10, '1980/07/07', 'never'), (2, 'André', 'Ampère', 423, 'piza', 8, '1950/07/08', 'gonna'), (3, 'NiELS', 'Böhr//((%%', 551, 'pizza', 8, '1990/07/09', 'give'), (4, 'PAUL', 'dirac$', 521, 'pizza', 8, '1954/07/10', 'you'), (5, 'Albert', 'Einstein', 634, 'pizza', 8, '1990/07/11', 'up'), (6, 'Galileo', '             GALiLEI', 672, 'arepa', 5, '1930/08/12', 'never'), (7, 'CaRL', 'Ga%%%uss', 323, 'taco', 3, '1970/07/13', 'gonna'), (8, 'David', 'H$$$ilbert', 624, 'taaaccoo', 3, '1950/07/14', 'let'), (9, 'Johannes', 'KEPLER', 735, 'taco', 3, '1920/04/22', 'you'), (10, 'JaMES', 'M$$ax%%well', 875, 'taco', 3, '1923/03/12', 'down'), (11, 'Isaac', 'Newton', 992, 'pasta', 9, '1999/02/15', 'never '), (12, 'Emmy%%', 'Nöether$', 234, 'pasta', 9, '1993/12/08', 'gonna'), (13, 'Max!!!', 'Planck!!!', 111, 'hamburguer', 4, '1994/01/04', 'run '), (14, 'Fred', 'Hoy&&&le', 553, 'pizzza', 8, '1997/06/27', 'around'), (15, '(((   Heinrich )))))', 'Hertz', 116, 'pizza', 8, '1956/11/30', 'and'), (16, 'William', 'Gilbert###', 886, 'BEER', 2, '1958/03/26', 'desert'), (17, 'Marie', 'CURIE', 912, 'Rice', 1, '2000/03/22', 'you'), (18, 'Arthur', 'COM%%%pton', 812, '110790', 5, '1899/01/01', '#'), (19, 'JAMES', 'Chadwick', 467, 'null', 10, '1921/05/03', '#')])
		assert (expected_df.collect() == actual_df.collect())
	@staticmethod
	def test_save_csv():
		actual_df =source_df.save.csv('test.csv')
		
	@staticmethod
	def test_save_json():
		actual_df =source_df.save.json('test.json')
		
	@staticmethod
	def test_save_parquet():
		actual_df =source_df.save.parquet('test.parquet')
		
