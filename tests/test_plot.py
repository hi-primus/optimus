# -*- coding: utf-8 -*-
# + {}
from pyspark.sql.types import *
import datetime
import sys

from optimus import Optimus
# -

op = Optimus()

source_df = op.create.df(
    [('names', StringType(), True), ('height(ft)', ShortType(), True), ('function', StringType(), True),
     ('rank', ByteType(), True), ('age', IntegerType(), True), ('weight(t)', FloatType(), True),
     ('japanese name', ArrayType(StringType(), True), True), ('last position seen', StringType(), True),
     ('date arrival', StringType(), True), ('last date seen', StringType(), True),
     ('attributes', ArrayType(FloatType(), True), True), ('DateType', DateType(), True),
     ('timestamp', TimestampType(), True), ('Cybertronian', BooleanType(), True),
     ('function(binary)', BinaryType(), True), ('NullType', NullType(), True)], [("Optim'us", 28, 'Leader', 10, 5000000,
                                                                                  4.300000190734863,
                                                                                  ['Inochi', 'Convoy'],
                                                                                  '19.442735,-99.201111', '1980/04/10',
                                                                                  '2016/09/10',
                                                                                  [8.53439998626709, 4300.0],
                                                                                  datetime.date(2016, 9, 10),
                                                                                  datetime.datetime(2014, 6, 24, 0, 0),
                                                                                  True, bytearray(b'Leader'), None), (
                                                                                     'bumbl#ebéé  ', 17, 'Espionage', 7,
                                                                                     5000000, 2.0,
                                                                                     ['Bumble', 'Goldback'],
                                                                                     '10.642707,-71.612534',
                                                                                     '1980/04/10',
                                                                                     '2015/08/10',
                                                                                     [5.334000110626221, 2000.0],
                                                                                     datetime.date(2015, 8, 10),
                                                                                     datetime.datetime(2014, 6, 24, 0,
                                                                                                       0),
                                                                                     True, bytearray(b'Espionage'),
                                                                                     None), (
                                                                                     'ironhide&', 26, 'Security', 7,
                                                                                     5000000, 4.0, ['Roadbuster'],
                                                                                     '37.789563,-122.400356',
                                                                                     '1980/04/10',
                                                                                     '2014/07/10',
                                                                                     [7.924799919128418, 4000.0],
                                                                                     datetime.date(2014, 6, 24),
                                                                                     datetime.datetime(2014, 6, 24, 0,
                                                                                                       0),
                                                                                     True, bytearray(b'Security'),
                                                                                     None), (
                                                                                     'Jazz', 13, 'First Lieutenant', 8,
                                                                                     5000000, 1.7999999523162842,
                                                                                     ['Meister'],
                                                                                     '33.670666,-117.841553',
                                                                                     '1980/04/10', '2013/06/10',
                                                                                     [3.962399959564209, 1800.0],
                                                                                     datetime.date(2013, 6, 24),
                                                                                     datetime.datetime(2014, 6, 24, 0,
                                                                                                       0),
                                                                                     True,
                                                                                     bytearray(b'First Lieutenant'),
                                                                                     None), (
                                                                                     'Megatron', None, 'None', 10,
                                                                                     5000000,
                                                                                     5.699999809265137, ['Megatron'],
                                                                                     None,
                                                                                     '1980/04/10', '2012/05/10',
                                                                                     [None, 5700.0],
                                                                                     datetime.date(2012, 5, 10),
                                                                                     datetime.datetime(2014, 6, 24, 0,
                                                                                                       0),
                                                                                     True, bytearray(b'None'), None), (
                                                                                     'Metroplex_)^$', 300,
                                                                                     'Battle Station',
                                                                                     8, 5000000, None, ['Metroflex'],
                                                                                     None,
                                                                                     '1980/04/10', '2011/04/10',
                                                                                     [91.44000244140625, None],
                                                                                     datetime.date(2011, 4, 10),
                                                                                     datetime.datetime(2014, 6, 24, 0,
                                                                                                       0),
                                                                                     True, bytearray(b'Battle Station'),
                                                                                     None)])

source_df.table()


class TestDataFrameCols(object):
    @staticmethod
    def test_correlation():

        try:
            source_df.plot.correlation(["age", "rank"])

        except RuntimeError:
            logging.exception('Error creating the json output.')
            sys.exit(1)

    @staticmethod
    def test_boxplot():

        try:
            source_df.plot.box(["age", "weight(t)"])

        except RuntimeError:
            logging.exception('Error creating the json output.')
            sys.exit(1)

    @staticmethod
    def test_scatterplot():

        try:
            source_df.plot.scatter(["age", "weight(t)"])

        except RuntimeError:
            logging.exception('Error creating the json output.')
            sys.exit(1)

    @staticmethod
    def test_hist():

        try:
            source_df.plot.hist("age")

        except RuntimeError:
            logging.exception('Error creating the json output.')
            sys.exit(1)

    @staticmethod
    def test_frequency():

        try:
            source_df.plot.frequency("age")

        except RuntimeError:
            logging.exception('Error creating the json output.')
            sys.exit(1)

    @staticmethod
    def test_qqplot():

        try:
            source_df.plot.qqplot("age")

        except RuntimeError:
            logging.exception('Error creating the json output.')
            sys.exit(1)