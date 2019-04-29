from pyspark.sql.types import *
from optimus import Optimus
from pyspark.sql import functions as F


class TestOptimus(object):
    @staticmethod
    def test_optimus_from_session():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName('abc').getOrCreate()
        df = spark.read.csv('examples/data/foo.csv', header=True)
        op = Optimus(spark)
        df.table()