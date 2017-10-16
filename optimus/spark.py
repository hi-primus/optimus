from pyspark.sql import SparkSession
from functools import lru_cache

@lru_cache(maxsize=None)
def get_spark():
    return (SparkSession.builder
                .master("local")
                .appName("gill")
                .getOrCreate())