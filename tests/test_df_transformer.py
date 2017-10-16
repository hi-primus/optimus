import pytest

from optimus.spark import get_spark
import optimus as op

class TestDataFrameTransformer(object):

    def test_lower_case(self):
        source_data = [
            ("BOB", 1),
            ("JoSe", 2)
        ]
        source_df = get_spark().createDataFrame(
            source_data,
            ["name", "age"]
        )

        transformer = op.DataFrameTransformer(source_df)
        actual_df = transformer.lower_case("*").get_data_frame

        expected_data = [
            ("bob", 1),
            ("jose", 2)
        ]
        expected_df = get_spark().createDataFrame(
            expected_data,
            ["name", "age"]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_upper_case(self):
        source_data = [
            ("BOB", 1),
            ("JoSe", 2)
        ]
        source_df = get_spark().createDataFrame(
            source_data,
            ["name", "age"]
        )

        transformer = op.DataFrameTransformer(source_df)
        actual_df = transformer.upper_case("name").get_data_frame

        expected_data = [
            ("BOB", 1),
            ("JOSE", 2)
        ]
        expected_df = get_spark().createDataFrame(
            expected_data,
            ["name", "age"]
        )

        assert(expected_df.collect() == actual_df.collect())
