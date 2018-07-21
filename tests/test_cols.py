from optimus import Optimus
from pyspark.sql.types import *

op = Optimus()
spark = op.get_sc()


class TestDataFrameCols(object):

    def test_lower_case(self):
        source_df = op.create.df(
            rows=[
                ("BOB", 1),
                ("JoSe", 2)
            ],
            cols=[
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        actual_df = source_df.cols().lower("*")

        expected_df = op.create.df(
            rows=[
                ("bob", 1),
                ("jose", 2)
            ],
            cols=
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        assert (expected_df.collect() == actual_df.collect())

    def test_upper_case(self):
        source_df = op.create.df(
            rows=[
                ("BOB", 1),
                ("JoSe", 2)
            ],
            cols=
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        actual_df = source_df.cols().upper("name")

        expected_df = op.create.df(
            rows=[
                ("BOB", 1),
                ("JOSE", 2)
            ],
            cols=
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        assert (expected_df.collect() == actual_df.collect())

    def test_trim_col(self):
        source_df = op.create.df(
            rows=[
                ("  ron", 1),
                ("      bill     ", 2)
            ],
            cols=[
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        actual_df = source_df.cols().trim("name")

        expected_df = op.create.df(
            rows=[
                ("ron", 1),
                ("bill", 2)
            ],
            cols=[
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        assert (expected_df.collect() == actual_df.collect())

    def test_drop_col(self):
        source_df = op.create.df(
            rows=[
                ("happy", 1, 8),
                ("excited", 2, 8)
            ],
            cols=[
                ("emotion", StringType(), True),
                ("num1", IntegerType(), True),
                ("num2", IntegerType(), True)
            ]
        )

        actual_df = source_df.cols().drop("num1")

        expected_df = op.create.df(
            rows=[
                ("happy", 8),
                ("excited", 8)
            ],
            cols=
            [
                ("emotion", StringType(), True),
                ("num2", IntegerType(), True)
            ]
        )
        assert (expected_df.collect() == actual_df.collect())

    def test_replace_col(self):
        source_df = op.create.df(
            rows=[
                ("happy", 1),
                ("excited and happy", 2)
            ],
            cols=[
                ("emotion", StringType(), True),
                ("num1", IntegerType(), True)
            ]
        )

        transformer = op.DataFrameTransformer(source_df)
        actual_df = transformer.replace_col("happy", "elated", "emotion").df

        expected_df = op.create.df(
            rows=[
                ("elated", 1),
                ("excited and happy", 2)
            ],
            cols=[
                ("emotion", StringType(), True),
                ("num1", IntegerType(), True)
            ]
        )
        assert (expected_df.collect() == actual_df.collect())

    def test_set_col(self):
        source_df = op.create.df(
            rows=[
                ("cafe", 1),
                ("discoteca", 2)
            ],
            cols=[
                ("place", StringType(), True),
                ("num1", IntegerType(), True)
            ]
        )

        transformer = op.DataFrameTransformer(source_df)
        func = lambda num: num * 2
        actual_df = transformer.set_col("num1", func, "integer").df

        expected_df = op.create.df(
            rows=[
                ("cafe", 2),
                ("discoteca", 4)
            ],
            cols=[
                ("place", StringType(), True),
                ("num1", IntegerType(), True)
            ]
        )

        assert (expected_df.collect() == actual_df.collect())
