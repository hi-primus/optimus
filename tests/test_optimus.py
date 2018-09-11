import pandas as pd
from pyspark.sql.types import *

from optimus import Optimus

op = Optimus()


class TestDataFrameCols(object):

    @staticmethod
    def test_create_data_frames_one_column():
        source_df = op.create.df(
            rows=["Argenis", "Favio", "Matthew"],
            cols=["name"]
        )

        actual_df = source_df

        expected_df = op.create.df(
            rows=["Argenis", "Favio", "Matthew"],
            cols=["name"]
        )

        assert (expected_df.collect() == actual_df.collect())

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

    @staticmethod
    def test_create_data_frames_pandas():
        labels = ["name", "age"]

        data = [("BOB", 1),
                ("JoSe", 2)]

        # Create pandas dataframe
        pdf = pd.DataFrame.from_records(data, columns=labels)

        actual_df = op.create.df(pdf)

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
