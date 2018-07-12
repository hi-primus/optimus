from pyspark.sql import SparkSession
from functools import lru_cache
from pyspark import SparkContext


@lru_cache(maxsize=None)
def get_spark():
    return (SparkSession.builder
            .master("local")
            .appName("optimus")
            .getOrCreate())


def get_sc():
    return SparkContext.getOrCreate()
