import optimus as op
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col
import pyspark
import sys


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
        sys.exit(1)


def create_sql_df(spark_session):
    try:
        df = spark_session.createDataFrame([
            (0, 1.0, 3.0),
            (2, 2.0, 5.0)
        ], ["id", "v1", "v2"])
        return df
    except RuntimeError:
        sys.exit(1)


def create_vector_df(spark_session):
    try:
        df = spark_session.createDataFrame([
            (0, Vectors.dense([1.0, 0.5, -1.0]),),
            (1, Vectors.dense([2.0, 1.0, 1.0]),),
            (2, Vectors.dense([4.0, 10.0, 2.0]),)
        ], ["id", "features"])
        return df
    except RuntimeError:
        sys.exit(1)


def create_assembler_df(spark_session):
    try:
        df = spark_session.createDataFrame(
            [(0, 18, 1.0, Vectors.dense([0.0, 10.0, 0.5]), 1.0)],
            ["id", "hour", "mobile", "userFeatures", "clicked"])
        return df
    except RuntimeError:
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
        sys.exit(1)


def test_transformer(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        assert isinstance(transformer.get_data_frame, pyspark.sql.dataframe.DataFrame)
    except RuntimeError:
        sys.exit(1)


def test_trim_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.trim_col("*")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_drop_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.drop_col("country")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_keep_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.keep_col(['city', 'population'])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_replace_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.replace_col(search='Tokyo', change_to='Maracaibo', columns='city')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_delete_row(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        func = lambda pop: (pop > 6500000) & (pop <= 30000000)
        transformer.delete_row(func(col('population')))
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_set_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        func = lambda cell: (cell * 2) if (cell > 14000000) else cell
        transformer.set_col(['population'], func, 'integer')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_clear_accents(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.clear_accents(columns='*')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_remove_special_chars(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.remove_special_chars(columns=['city', 'country'])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_remove_special_chars_regex(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.remove_special_chars_regex(columns=['city', 'country'], regex='[^\w\s]')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_rename_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        names = [('city', 'villes')]
        transformer.rename_col(names)
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_lookup(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.lookup('city', "Caracas", ['Caracas', 'Ccs'])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_move_col(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.move_col('city', 'country', position='after')
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_date_transform(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_other_df(spark_session))
        transformer.date_transform(columns="dates",
                                   current_format="yyyy/mm/dd",
                                   output_format="dd-mm-yyyy")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_get_frequency(spark_session):
    try:
        analyzer = op.DataFrameAnalyzer(create_another_df(spark_session))
        analyzer.get_frequency(columns="city", sort_by_count=True)
        assert_spark_df(analyzer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_to_csv(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_other_df(spark_session))
        transformer.to_csv("test.csv")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_read_csv():
    try:
        tools = op.Utilities()
        df = tools.read_csv("tests/foo.csv", header="true", sep=",")
        assert_spark_df(df)
    except RuntimeError:
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
        sys.exit(1)


def test_string_to_index(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.string_to_index(["city", "country"])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_index_to_string(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_df(spark_session))
        transformer.string_to_index(["city", "country"])
        transformer.index_to_string(["city_index", "country_index"])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_one_hot_encoder(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_sql_df(spark_session))
        transformer.one_hot_encoder(["id"])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_sql(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_sql_df(spark_session))
        transformer.sql("SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_assembler(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_assembler_df(spark_session))
        transformer.vector_assembler(["hour", "mobile", "userFeatures"])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def test_normalizer(spark_session):
    try:
        transformer = op.DataFrameTransformer(create_vector_df(spark_session))
        transformer.normalizer(["features"])
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)


def replace_na():
    try:
        tools = op.Utilities()
        df = tools.read_csv("tests/impute_data.csv", header="true", sep=",")
        transformer = op.DataFrameTransformer(df)
        transformer.replace_na(10, columns="*")
        assert_spark_df(transformer.get_data_frame)
    except RuntimeError:
        sys.exit(1)
