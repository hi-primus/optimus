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
            logging.exception('Error executing the profiler.')
            sys.exit(1)


def test_save_to_html():
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
        op.profiler.to_file("output.html", "html")

    except RuntimeError:
        logging.exception('Error creating the html output.')
        sys.exit(1)


def test_save_to_json():
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
        op.profiler.to_file("output.json", "json")

    except RuntimeError:
        logging.exception('Error creating the json output.')
        sys.exit(1)
