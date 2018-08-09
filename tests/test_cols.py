from optimus import Optimus
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

op = Optimus()
sc = op.sc


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

        actual_df = source_df.cols.lower("*")

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

        actual_df = source_df.cols.upper("name")

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

        actual_df = source_df.cols.trim("name")

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
        expected_df.show()
        actual_df.show()

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

        actual_df = source_df.cols.drop("num1")

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

        actual_df = source_df.cols.replace("emotion", [("happy", "elated")])

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

    def test_set_apply_exp(self):
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

        def func(col_name, attrs):
            return F.col(col_name) * 2

        actual_df = source_df.cols.apply_exp("num1", func)

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

    def test_append_number(self):
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

        actual_df = source_df.cols.append("num3", 1)

        expected_df = op.create.df(
            rows=[
                ("happy", 1, 8, 1),
                ("excited", 2, 8, 1)
            ],
            cols=[
                ("emotion", StringType(), True),
                ("num1", IntegerType(), True),
                ("num2", IntegerType(), True),
                ("num3", IntegerType(), True)
            ]
        )

        assert (actual_df.collect() == expected_df.collect())

    def test_append_advanced(self):
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

        actual_df = source_df.cols.append([("new_col_4", "test"),
                                           ("new_col_5", source_df['num1'] * 2),
                                           ("new_col_6", [1, 2, 3])
                                           ])

        expected_df = op.create.df(
            rows=[
                ("happy", 1, 8, "test", 2, [1, 2, 3]),
                ("excited", 2, 8, "test", 4, [1, 2, 3])
            ],
            cols=[
                ("emotion", StringType(), True),
                ("num1", IntegerType(), True),
                ("num2", IntegerType(), True),
                ("new_col_4", StringType(), True),
                ("new_col_5", IntegerType(), True),
                ("new_col_6", ArrayType(IntegerType()), True)
            ]
        )

        assert (actual_df.collect() == expected_df.collect())
