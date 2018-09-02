from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import *

from optimus import Optimus

op = Optimus()


class TestDataFrameCols(object):

    @staticmethod
    def test_create_data_frames_plain():
        source_df = op.create.df(
            rows=[
                ("BOB", 1),
                ("JoSe", 2)
            ],
            cols=[
                "name",
                "age"
            ]
        )

        actual_df = source_df

        expected_df = op.create.df(
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

        assert (expected_df.collect() == actual_df.collect())

    @staticmethod
    def test_create_data_frames_with_datatypes():
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

        actual_df = source_df

        expected_df = op.create.df(
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

        assert (expected_df.collect() == actual_df.collect())

    @staticmethod
    def test_create_data_frames_nullable():
        source_df = op.create.df(
            rows=[
                ("BOB", 1),
                ("JoSe", 2)
            ],
            cols=[
                ("name", StringType()),
                ("age", IntegerType())
            ]
        )

        actual_df = source_df

        expected_df = op.create.df(
            rows=[
                ("BOB", 1),
                ("JoSe", 2)
            ],
            cols=
            [
                ("name", StringType(), True),
                ("age", IntegerType(), True)
            ]
        )

        assert (expected_df.collect() == actual_df.collect())
