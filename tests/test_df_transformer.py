from optimus.spark import get_spark
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

        transformer = op.DataFrameTransformer(source_df)
        actual_df = transformer.lower_case("*").get_data_frame

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

        transformer = op.DataFrameTransformer(source_df)
        actual_df = transformer.upper_case("name").get_data_frame

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
