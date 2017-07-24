# Importing DataFrameTransformer library
from optimus.DfTransf import DataFrameTransformer
# Import spark types
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

# Create contexts
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

sc = SparkContext()
sqlContext = SQLContext(sc)

# Define Dataframe
schema = StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("population", IntegerType(), True)])

countries = ['Japan', 'USA', 'France', 'Spain']
cities = ['Tokyo', 'New York', '   Paris   ', 'Madrid']
population = [37800000, 19795791, 12341418, 6489162]

# Dataframe:
df = sqlContext.createDataFrame(list(zip(cities, countries, population)), schema=schema)

assert (isinstance(df, pyspark.sql.dataframe.DataFrame))
