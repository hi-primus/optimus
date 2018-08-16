from optimus import Optimus
from pyspark.sql.types import *



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
