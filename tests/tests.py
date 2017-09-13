import airbrake
import optimus as op
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
import sys


logger = airbrake.getLogger()


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
    except RuntimeError as err:
        logger.exception('Could not run trim_col().')
        print(err)
        sys.exit(1)
