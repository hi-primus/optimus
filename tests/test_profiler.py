import logging
import sys
from datetime import date, datetime

from pyspark.sql.types import *

from optimus import Optimus

op = Optimus()

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.INFO)

source_df = op.create.df(
    [
        ("names", "str", True),
        ("height(ft)", "int", True),
        ("function", "str", True),
        ("rank", "int", True),
        ("age", "int", True),
        ("weight(t)", "float", True),
        ("japanese name", ArrayType(StringType()), True),
        ("last position seen", "str", True),
        ("date arrival", "str", True),
        ("last date seen", "str", True),
        ("attributes", ArrayType(FloatType()), True),
        ("DateType", DateType()),
        ("Timestamp", TimestampType()),
        ("Cybertronian", "bool", True),
        ("function(binary)", "binary", False),
        ("NullType", "null", True),

    ],
    [
        ("Optim'us", 28, "Leader", 10, 5000000, 4.30, ["Inochi", "Convoy"], "19.442735,-99.201111", "1980/04/10",
         "2016/09/10", [8.5344, 4300.0], date(2016, 9, 10), datetime(2014, 6, 24), True, bytearray("Leader", "utf-8"),
         None),
        ("bumbl#ebéé  ", 17, "Espionage", 7, 5000000, 2.0, ["Bumble", "Goldback"], "10.642707,-71.612534", "1980/04/10",
         "2015/08/10", [5.334, 2000.0], date(2015, 8, 10), datetime(2014, 6, 24), True, bytearray("Espionage", "utf-8"),
         None),
        ("ironhide&", 26, "Security", 7, 5000000, 4.0, ["Roadbuster"], "37.789563,-122.400356", "1980/04/10",
         "2014/07/10", [7.9248, 4000.0], date(2014, 6, 24), datetime(2014, 6, 24), True, bytearray("Security", "utf-8"),
         None),
        ("Jazz", 13, "First Lieutenant", 8, 5000000, 1.80, ["Meister"], "33.670666,-117.841553", "1980/04/10",
         "2013/06/10", [3.9624, 1800.0], date(2013, 6, 24), datetime(2014, 6, 24), True,
         bytearray("First Lieutenant", "utf-8"), None),
        ("Megatron", None, "None", 10, 5000000, 5.70, ["Megatron"], None, "1980/04/10", "2012/05/10", [None, 5700.0],
         date(2012, 5, 10), datetime(2014, 6, 24), True, bytearray("None", "utf-8"), None),
        ("Metroplex_)^$", 300, "Battle Station", 8, 5000000, None, ["Metroflex"], None, "1980/04/10", "2011/04/10",
         [91.44, None], date(2011, 4, 10), datetime(2014, 6, 24), True, bytearray("Battle Station", "utf-8"), None),
        ("1", 2, "3", 4, 5, 6.0, ["7"], 8, "1980/04/10", "2011/04/10",
         [11.0], date(2011, 4, 10), datetime(2014, 6, 24), True, bytearray("15", "utf-8"), None)
    ], infer_schema=True)


class TestProfiler(object):
    @staticmethod
    def test_run():

        try:
            op.profiler.run(source_df, "*")

        except RuntimeError:
            logging.exception('Error executing the profiler.')
            sys.exit(1)


def test_save_to_html():
    try:
        op.profiler.run(source_df, "*").to_file("output.html", "html")

    except RuntimeError:
        logging.exception('Error creating the html output.')
        sys.exit(1)


def test_save_to_json():
    try:
        op.profiler.run(source_df, "*").to_file("output.json", "json")

    except RuntimeError:
        logging.exception('Error creating the json output.')
        sys.exit(1)
