import airbrake
import optimus as op
# Importing sql types
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


logger = airbrake.getLogger()


def test_spark_context_fixture(spark_context):
    test_rdd = spark_context.parallelize([1, 2, 3, 4])

    try:
        assert test_rdd.count() == 4
    except AssertionError:
        logger.exception('Wrong count for RDD.')


def test_trim(spark_session):
    try:
        # Building a simple dataframe:
        schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("population", IntegerType(), True)])

        countries = ['Colombia   ', 'US@A', 'Brazil', 'Spain']
        cities = ['Bogotá', 'New York', '   São Paulo   ', '~Madrid']
        population = [37800000, 19795791, 12341418, 6489162]

        # Dataframe:
        df = spark_session.createDataFrame(list(zip(cities, countries, population)), schema=schema)
        transformer = op.DataFrameTransformer(df)
        transformer.trim_col("*")
    except RuntimeError:
        logger.exception('Could not run trim_col().')
