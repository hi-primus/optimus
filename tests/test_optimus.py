from pyspark.sql.types import *
from optimus import Optimus
import datetime
from pyspark.sql import functions as F

op = Optimus(master='local')


class TestOptimus(object):
    @staticmethod
    def test_create_df_one_column():
        source_df = op.create.df([('name', StringType(), True)], [('Argenis',), ('Favio',), ('Matthew',)])
        actual_df = source_df
        expected_df = op.create.df([('name', StringType(), True)], [('Argenis',), ('Favio',), ('Matthew',)])
        assert (expected_df.collect() == actual_df.collect())

    @staticmethod
    def test_create_df_plain():
        source_df = op.create.df([('name', StringType(), True), ('age', IntegerType(), True)],
                                 [('BOB', 1), ('JoSe', 2)])
        actual_df = source_df
        expected_df = op.create.df([('name', StringType(), True), ('age', IntegerType(), True)],
                                   [('BOB', 1), ('JoSe', 2)])
        assert (expected_df.collect() == actual_df.collect())

    @staticmethod
    def test_create_df_plain_infer_false():
        source_df = op.create.df([('name', StringType(), True), ('age', StringType(), True)],
                                 [('BOB', '1'), ('JoSe', '2')])
        actual_df = source_df
        expected_df = op.create.df([('name', StringType(), True), ('age', StringType(), True)],
                                   [('BOB', '1'), ('JoSe', '2')])
        assert (expected_df.collect() == actual_df.collect())

    @staticmethod
    def test_create_df_with_data_types():
        source_df = op.create.df([('name', StringType(), True), ('age', IntegerType(), False)],
                                 [('BOB', 1), ('JoSe', 2)])
        actual_df = source_df
        expected_df = op.create.df([('name', StringType(), True), ('age', IntegerType(), False)],
                                   [('BOB', 1), ('JoSe', 2)])
        assert (expected_df.collect() == actual_df.collect())

    @staticmethod
    def test_create_df_nullable():
        source_df = op.create.df([('name', StringType(), True), ('age', IntegerType(), True)],
                                 [('BOB', 1), ('JoSe', 2)])
        actual_df = source_df
        expected_df = op.create.df([('name', StringType(), True), ('age', IntegerType(), True)],
                                   [('BOB', 1), ('JoSe', 2)])
        assert (expected_df.collect() == actual_df.collect())
