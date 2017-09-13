import airbrake
import optimus as op
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
import pyspark
import sys


logger = airbrake.getLogger()


def create_df(spark_session):
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
        assert isinstance(df, pyspark.sql.dataframe.DataFrame)
        return df
    except RuntimeError as err:
        logger.exception('Could not run trim_col().')
        print(err)
        sys.exit(1)


def test_trim_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.trim_col("*")
        assert isinstance(transformer.get_data_frame(), pyspark.sql.dataframe.DataFrame)
    except RuntimeError as err:
        logger.exception('Could not run trim_col().')
        print(err)
        sys.exit(1)


def test_drop_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.drop_col("country")
        assert isinstance(transformer.get_data_frame(), pyspark.sql.dataframe.DataFrame)
    except RuntimeError as err:
        logger.exception('Could not run drop_col().')
        print(err)
        sys.exit(1)
