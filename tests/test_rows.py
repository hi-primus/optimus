from optimus import Optimus
from pyspark.sql.types import *
from optimus.functions import abstract_udf as audf


op = Optimus()
sc = op.sc

source_df = op.create.df([
    ("words", "str", True),
    ("num", "int", True),
    ("animals", "str", True),
    ("thing", StringType(), True),
    ("second", "int", True),
    ("filter", StringType(), True)
],
    [
        ("  I like     fish  ", 1, "dog dog", "housé", 5, "a"),
        ("    zombies", 2, "cat", "tv", 6, "b"),
        ("simpsons   cat lady", 2, "frog", "table", 7, "1"),
        (None, 3, "eagle", "glass", 8, "c"),
    ])


class TestDataFrameRows(object):

    def test_append(self):
        actual_df = source_df.rows.append(["this is a word", 2, "this is an animal",
                                           "this is a thing", 64, "this is a filter"])

        expected_df = op.create.df([
            ("words", "str", True),
            ("num", "int", True),
            ("animals", "str", True),
            ("thing", StringType(), True),
            ("second", "int", True),
            ("filter", StringType(), True)
        ],
            [
                ("  I like     fish  ", 1, "dog dog", "housé", 5, "a"),
                ("    zombies", 2, "cat", "tv", 6, "b"),
                ("simpsons   cat lady", 2, "frog", "table", 7, "1"),
                (None, 3, "eagle", "glass", 8, "c"),
                ("this is a word", 2, "this is an animal",
                 "this is a thing", 64, "this is a filter")
            ])

        assert (expected_df.collect() == actual_df.collect())

    def test_filter(self):
        actual_df = source_df.rows.rows.filter(source_df["num"] == 1)

        expected_df = op.create.df([
            ("words", "str", True),
            ("num", "int", True),
            ("animals", "str", True),
            ("thing", StringType(), True),
            ("second", "int", True),
            ("filter", StringType(), True)
        ],
            [
                ("  I like     fish  ", 1, "dog dog", "housé", 5, "a")
            ])

        assert (expected_df.collect() == actual_df.collect())

    def test_filter_by_dtypes(self):
        actual_df = source_df.rows.rows.filter_by_dtypes("filter", "integer")

        expected_df = op.create.df([
            ("words", "str", True),
            ("num", "int", True),
            ("animals", "str", True),
            ("thing", StringType(), True),
            ("second", "int", True),
            ("filter", StringType(), True)
        ],
            [
                ("simpsons   cat lady", 2, "frog", "table", 7, "1")
            ])

        assert (expected_df.collect() == actual_df.collect())

    def test_drop_by_dtypes(self):
        actual_df = source_df.rows.rows.drop_by_dtypes("filter", "integer")

        expected_df = op.create.df([
            ("words", "str", True),
            ("num", "int", True),
            ("animals", "str", True),
            ("thing", StringType(), True),
            ("second", "int", True),
            ("filter", StringType(), True)
        ],
            [
                ("  I like     fish  ", 1, "dog dog", "housé", 5, "a"),
                ("    zombies", 2, "cat", "tv", 6, "b"),
                (None, 3, "eagle", "glass", 8, "c")
            ])

        assert (expected_df.collect() == actual_df.collect())

    def test_drop(self):
        actual_df = source_df.rows.drop((source_df["num"] == 2) | (source_df["second"] == 5))

        expected_df = op.create.df([
            ("words", "str", True),
            ("num", "int", True),
            ("animals", "str", True),
            ("thing", StringType(), True),
            ("second", "int", True),
            ("filter", StringType(), True)
        ],
            [
                (None, 3, "eagle", "glass", 8, "c")
            ])

        assert (expected_df.collect() == actual_df.collect())

    def test_drop_audf(self):
        def func_data_type(value, attr):
            return value > 1

        actual_df = source_df.rows.drop(audf("num", func_data_type, "boolean"))

        expected_df = op.create.df([
            ("words", "str", True),
            ("num", "int", True),
            ("animals", "str", True),
            ("thing", StringType(), True),
            ("second", "int", True),
            ("filter", StringType(), True)
        ],
            [
                ("  I like     fish  ", 1, "dog dog", "housé", 5, "a")
            ])

        assert (expected_df.collect() == actual_df.collect())
