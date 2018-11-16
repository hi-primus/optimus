import logging
import sys

from pyspark.sql.types import *

from optimus import Optimus

op = Optimus()

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.INFO)


class TestProfiler(object):
    @staticmethod
    def test_run():
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
        try:
            op.profiler.run(source_df, "*")

        except RuntimeError:
            logging.exception('Could not create dataframe.')
            sys.exit(1)
