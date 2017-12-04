from optimus.spark import get_spark
from optimus.df_transforms import *
import optimus as op
from quinn.extensions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit


class TestDfTransforms:

    def test_reorder_columns(self):
        source_df = get_spark().create_df(
            [
                ("bob", 1),
                ("jose", 2),
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False),
            ]
        )

        actual_df = reorder_columns("age", "name")(source_df)

        expected_df = get_spark().create_df(
            [
                (1, "bob"),
                (2, "jose"),
            ],
            [
                ("age", IntegerType(), False),
                ("name", StringType(), True),
            ]
        )

        assert expected_df.collect() == actual_df.collect()

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

        assert expected_df.collect() == actual_df.collect()


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

        assert expected_df.collect() == actual_df.collect()


    def test_remove_chars(self):
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

        special_chars = ("!", "&", ".")
        actual_df = remove_chars(["name"], special_chars)(source_df)

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

    def test_multi_remove_chars(self):
        source_df = get_spark().create_df(
            [
                ("bob!!", "!!ayo&&", 1),
                ("jo..se&&", "s!u!p", 2)
            ],
            [
                ("name", StringType(), True),
                ("greeting", StringType(), True),
                ("age", IntegerType(), False),
            ]
        )

        special_chars = ("!", "&", ".")
        actual_df = remove_chars(["name", "greeting"], special_chars)(source_df)

        expected_df = get_spark().create_df(
            [
                ("bob", "ayo", 1),
                ("jose", "sup", 2)
            ],
            [
                ("name", StringType(), True),
                ("greeting", StringType(), True),
                ("age", IntegerType(), False),
            ]
        )

        assert expected_df.collect() == actual_df.collect()


    def test_chaining_with_new_interface(self):
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

        special_chars = ("!", "&", ".")
        actual_df = (source_df
            .transform(remove_chars(["name"], special_chars))
            .withColumn("fun", lit("&AWESOME!"))
            .transform(remove_chars(["fun"], special_chars))
            .transform(lower_case("fun")))

        expected_df = get_spark().create_df(
            [
                ("bob", 1, "awesome"),
                ("jose", 2, "awesome")
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False),
                ("fun", StringType(), True)
            ]
        )

        assert expected_df.collect() == actual_df.collect()


    def test_existing_interface(self):
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

        transformer1 = op.DataFrameTransformer(source_df)
        df1 = transformer1.remove_special_chars("name").df
        df2 = df1.withColumn("fun", lit("&AWESOME!"))
        transformer2 = op.DataFrameTransformer(df2)
        actual_df = transformer2.remove_special_chars("fun").lower_case("fun").df

        expected_df = get_spark().create_df(
            [
                ("bob", 1, "awesome"),
                ("jose", 2, "awesome")
            ],
            [
                ("name", StringType(), True),
                ("age", IntegerType(), False),
                ("fun", StringType(), True)
            ]
        )

        assert expected_df.collect() == actual_df.collect()
