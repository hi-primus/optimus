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


def create_other_df(spark_session):
    try:
        # Building a simple dataframe:
        schema = StructType([
            StructField("city", StringType(), True),
            StructField("dates", StringType(), True),
            StructField("population", IntegerType(), True)])

        dates = ['1991/02/25', '1998/05/10', '1993/03/15', '1992/07/17']
        cities = ['Caracas', 'Ccs', '   São Paulo   ', '~Madrid']
        population = [37800000, 19795791, 12341418, 6489162]

        # Dataframe:
        df = spark_session.createDataFrame(list(zip(cities, dates, population)), schema=schema)
        assert_spark_df(df)
        return df
    except RuntimeError:
        logger.exception('Could not create other dataframe.')
        sys.exit(1)


def create_another_df(spark_session):
    try:
        # Building a simple dataframe:
        schema = StructType([
            StructField("city", StringType(), True),
            StructField("dates", StringType(), True),
            StructField("population", IntegerType(), True)])

        dates = ['1991/02/25', '1998/05/10', '1993/03/15', '1992/07/17']
        cities = ['Caracas', 'Caracas', '   Maracaibo   ', 'Madrid']
        population = [37800000, 19795791, 12341418, 6489162]

        # Dataframe:
        df = spark_session.createDataFrame(list(zip(cities, dates, population)), schema=schema)
        assert_spark_df(df)
        return df
    except RuntimeError:
        logger.exception('Could not create other dataframe.')
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


def test_set_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        func = lambda cell: (cell * 2) if (cell > 14000000) else cell
        transformer.set_col(['population'], func, 'integer')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run set_col().')
        sys.exit(1)


def test_clear_accents(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.clear_accents(columns='*')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run clear_accents().')
        sys.exit(1)


def test_remove_special_chars(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.remove_special_chars(columns=['city', 'country'])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run remove_special_chars().')
        sys.exit(1)


def test_remove_special_chars_regex(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.remove_special_chars_regex(columns=['city', 'country'], regex='[^\w\s]')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run remove_special_chars_regex().')
        sys.exit(1)


def test_rename_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        names = [('city', 'villes')]
        transformer.rename_col(names)
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run rename_col().')
        sys.exit(1)


def test_lookup(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.lookup('city', "Caracas", ['Caracas', 'Ccs'])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run lookup().')
        sys.exit(1)


def test_move_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.move_col('city', 'country', position='after')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run move_col().')
        sys.exit(1)


def test_date_transform(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_other_df(spark_session))
        transformer.date_transform(columns="dates",
                                   current_format="yyyy/mm/dd",
                                   output_format="dd-mm-yyyy")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run date_transform().')
        sys.exit(1)


def test_get_frequency(spark_session):
    try:
        analyzer = op.DataFrameAnalyzer(create_another_df(spark_session))
        analyzer.get_frequency(columns="city", sort_by_count=True)
        assert_spark_df(analyzer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run get_frequency().')
        sys.exit(1)


def test_to_csv(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_other_df(spark_session))
        transformer.to_csv("test.csv")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        logger.exception('Could not run to_csv().')
        sys.exit(1)


def test_read_csv():
    try:
        tools = op.Utilities()
        df = tools.read_csv("tests/foo.csv", header="true", sep=",")
        assert_spark_df(df)
    except RuntimeError:
        logger.exception('Could not run read_csv().')
        sys.exit(1)


def test_create_data_frame():
    try:
        tools = op.Utilities()
        data = [('Japan', 'Tokyo', 37800000), ('USA', 'New York', 19795791), ('France', 'Paris', 12341418),
                ('Spain', 'Madrid', 6489162)]
        names = ["country", "city", "population"]
        df = tools.create_data_frame(data=data, names=names)
        assert_spark_df(df)
    except RuntimeError:
        logger.exception('Could not run create_data_frame().')
        sys.exit(1)


def test_indexer(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.indexer(["city", "country"])
        assert_spark_df(transformer.get_data_frame)
        assert transformer.get_data_frame.columns == ['country', 'city', 'population', 'city_index', 'country_index']
    except RuntimeError:
        logger.exception('Could not run indexer().')
        sys.exit(1)
