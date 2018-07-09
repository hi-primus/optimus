from optimus.spark import get_spark
from optimus.df_transformer import *
import optimus as op
from quinn.extensions import *
from pyspark.sql.types import *


class TestDataFrameTransformer(object):

    def test_lower_case(self):
        source_df = get_spark().create_df(
            [
                ("BOB", 1),
                ("JoSe", 2)
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        actual_df = source_df.transform(lower_case(["name"]))

        expected_df = get_spark().create_df(
            [
                ("bob", 1),
                ("jose", 2)
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        assert expected_df.collect() == actual_df.collect()

    def test_upper_case(self):
        source_df = get_spark().create_df(
            [
                ("BOB", 1),
                ("JoSe", 2)
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        actual_df = source_df.transform(upper_case(["name"]))

        expected_df = get_spark().create_df(
            [
                ("BOB", 1),
                ("JOSE", 2)
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_trim_col(self):
        source_df = get_spark().create_df(
            [
                ("  ron", 1),
                ("      bill     ", 2)
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        transformer = op.DataFrameTransformer(source_df)
        actual_df = transformer.trim_col("name").df

        expected_df = get_spark().create_df(
            [
                ("ron", 1),
                ("bill", 2)
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_drop_col(self):
        source_df = get_spark().create_df(
            [
                ("happy", 1, 8),
                ("excited", 2, 8)
            ],
            [
                ("emotion", StringType(), True),
                ("num1", IntegerType(), True),
                ("num2", IntegerType(), True)
            ]
        )

        transformer = op.DataFrameTransformer(source_df)
        actual_df = transformer.drop_col("num1").df

        expected_df = get_spark().create_df(
            [
                ("happy", 8),
                ("excited", 8)
            ],
            [
                ("emotion", StringType(), True),
                ("num2", IntegerType(), True)
            ]
        )
        assert(expected_df.collect() == actual_df.collect())

    def test_replace_col(self):
        source_df = get_spark().create_df(
            [
                ("happy", 1),
                ("excited and happy", 2)
            ],
            [
                ("emotion", StringType(), True),
                ("num1", IntegerType(), True)
            ]
        )

        transformer = op.DataFrameTransformer(source_df)
        actual_df = transformer.replace_col("happy", "elated", "emotion").df

        expected_df = get_spark().create_df(
            [
                ("elated", 1),
                ("excited and happy", 2)
            ],
            [
                ("emotion", StringType(), True),
                ("num1", IntegerType(), True)
            ]
        )
        assert(expected_df.collect() == actual_df.collect())

    def test_set_col(self):
        source_df = get_spark().create_df(
            [
                ("cafe", 1),
                ("discoteca", 2)
            ],
            [
                ("place", StringType(), True),
                ("num1", IntegerType(), True)
            ]
        )

        transformer = op.DataFrameTransformer(source_df)
        func = lambda num: num * 2
        actual_df = transformer.set_col("num1", func, "integer").df

        expected_df = get_spark().create_df(
            [
                ("cafe", 2),
                ("discoteca", 4)
            ],
            [
                ("place", StringType(), True),
                ("num1", IntegerType(), True)
            ]
        )

        assert(expected_df.collect() == actual_df.collect())
