import numpy as np
from pyspark.sql.types import *

from optimus import Optimus

nan = np.nan
import datetime

op = Optimus(master='local')
source_df = op.create.df(
    [('names', StringType(), True), ('height(ft)', ShortType(), True), ('function', StringType(), True),
     ('rank', ByteType(), True), ('age', IntegerType(), True), ('weight(t)', FloatType(), True),
     ('japanese name', ArrayType(StringType(), True), True), ('last position seen', StringType(), True),
     ('date arrival', StringType(), True), ('last date seen', StringType(), True),
     ('attributes', ArrayType(FloatType(), True), True), ('Date Type', DateType(), True),
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
meta_value = {"key": "value"}


class Test_meta(object):

    @staticmethod
    def test_cols_set_meta():
        actual_df = source_df.cols.set_meta("height(ft)", value=meta_value)
        actual_json = actual_df._jdf.schema().json()
        expected_json = '{"type":"struct","fields":[{"name":"names","type":"string","nullable":true,"metadata":{}},{"name":"height(ft)","type":"short","nullable":true,"metadata":{"key":"value"}},{"name":"function","type":"string","nullable":true,"metadata":{}},{"name":"rank","type":"byte","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"weight(t)","type":"float","nullable":true,"metadata":{}},{"name":"japanese name","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}},{"name":"last position seen","type":"string","nullable":true,"metadata":{}},{"name":"date arrival","type":"string","nullable":true,"metadata":{}},{"name":"last date seen","type":"string","nullable":true,"metadata":{}},{"name":"attributes","type":{"type":"array","elementType":"float","containsNull":true},"nullable":true,"metadata":{}},{"name":"Date Type","type":"date","nullable":true,"metadata":{}},{"name":"timestamp","type":"timestamp","nullable":true,"metadata":{}},{"name":"Cybertronian","type":"boolean","nullable":true,"metadata":{}},{"name":"function(binary)","type":"binary","nullable":true,"metadata":{}},{"name":"NullType","type":"null","nullable":true,"metadata":{}}]}'
        assert (actual_json == expected_json)

    @staticmethod
    def test_cols_get_meta():
        actual_df = source_df.cols.set_meta("height(ft)", value=meta_value)
        actual_value = actual_df.cols.get_meta("height(ft)")
        expected_value = meta_value
        assert (actual_value == expected_value)

    @staticmethod
    def test_df_set_meta():
        actual_df = source_df.set_meta(value=meta_value)
        actual_json = actual_df.schema[-1].metadata
        expected_json = {"key": "value"}
        assert (actual_json == expected_json)

    @staticmethod
    def test_df_get_meta():
        source_df.schema[-1].metadata = meta_value
        actual_value = source_df.get_meta()
        expected_value = {"key": "value"}
        assert (actual_value == expected_value)
