import airbrake
import optimus as op
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.functions import col
import pyspark
import sys


logger = airbrake.getLogger()


def assert_spark_df(df):
    assert (isinstance(df, pyspark.sql.dataframe.DataFrame))


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
        assert_spark_df(df)
        return df
    except RuntimeError:
        logger.exception('Could not create dataframe.')
        sys.exit(1)


def test_transformer(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        assert isinstance(transformer.get_data_frame, pyspark.sql.dataframe.DataFrame)
    except RuntimeError:
        logger.exception('Could not create transformer.')
        sys.exit(1)


def test_trim_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.trim_col("*")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run trim_col().')
        sys.exit(1)


def test_drop_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.drop_col("country")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run drop_col().')
        sys.exit(1)


def test_keep_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.keep_col(['city', 'population'])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run keep_col().')
        sys.exit(1)


def test_replace_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.replace_col(search='Tokyo', change_to='Maracaibo', columns='city')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run replace_col().')
        sys.exit(1)


def test_delete_row(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        func = lambda pop: (pop > 6500000) & (pop <= 30000000)
        transformer.delete_row(func(col('population')))
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run delete_row().')
        sys.exit(1)
