from optimus import Optimus
from pyspark.sql.types import *

op = Optimus()

source_df = op.create.df(
    [('names', StringType(), True), ('height', StringType(), True), ('function', StringType(), True),
     ('rank', StringType(), True)], [('bumbl#ebéé  ', '17.5', 'Espionage', '7'), ("Optim'us", '28.0', 'Leader', '10'),
                                     ('ironhide&', '26.0', 'Security', '7'), ('Jazz', '13.0', 'First Lieutenant', '8'),
                                     ('Megatron', None, 'None', None)])


class TestDataFrameExtensions(object):
    @staticmethod
    def test_melt():
        actual_df = source_df.melt(id_vars=['names'], value_vars=['height', 'function', 'rank'])

        expected_df = op.create.df(
            [('names', StringType(), True), ('variable', StringType(), False), ('value', StringType(), True)],
            [('bumbl#ebéé  ', 'height', '17.5'), ('bumbl#ebéé  ', 'function', 'Espionage'),
             ('bumbl#ebéé  ', 'rank', '7'), ("Optim'us", 'height', '28.0'), ("Optim'us", 'function', 'Leader'),
             ("Optim'us", 'rank', '10'), ('ironhide&', 'height', '26.0'), ('ironhide&', 'function', 'Security'),
             ('ironhide&', 'rank', '7'), ('Jazz', 'height', '13.0'), ('Jazz', 'function', 'First Lieutenant'),
             ('Jazz', 'rank', '8'), ('Megatron', 'height', None), ('Megatron', 'function', 'None'),
             ('Megatron', 'rank', None)])

        assert (expected_df.collect() == actual_df.collect())
