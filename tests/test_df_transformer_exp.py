from optimus.spark import get_spark
from optimus.df_transformer_exp import *
import optimus as op
from quinn.extensions import *
from pyspark.sql.types import *


class TestDfTransformerExp(object):

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

        actual_df = lower_case("name")(source_df)

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

        assert(expected_df.collect() == actual_df.collect())


    def test_trim_col(self):
        source_df = get_spark().create_df(
            [
                ("  BOB   ", 1),
                (" J oSe", 2)
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        actual_df = trim_col("name")(source_df)

        expected_df = get_spark().create_df(
            [
                ("BOB", 1),
                ("J oSe", 2)
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        assert(expected_df.collect() == actual_df.collect())


    def test_remove_special_chars(self):
        source_df = get_spark().create_df(
            [
                ("bob!!", 1),
                ("jo..se&&", 2)
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False)
            ]
        )

        actual_df = remove_special_chars("name")(source_df)

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

        assert(expected_df.collect() == actual_df.collect())
