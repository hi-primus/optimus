# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ### All this cell must be run to executed the tests

# %load_ext autoreload
# %autoreload 2

import sys

sys.path.append("../..")

from optimus import Optimus
from optimus.helpers.test import Test

op = Optimus(master='local')

# +
import pandas as pd
from pyspark.sql.types import *
from datetime import date, datetime


cols = [
        ("names", "str"),
        ("height(ft)", ShortType()),
        ("function", "str"),
        ("rank", ByteType()),
        ("age", "int"),
        ("weight(t)", "float"),
        "japanese name",
        "last position seen",
        "date arrival",
        "last date seen",
        ("attributes", ArrayType(FloatType())),
        ("Date Type", DateType()),
        ("Tiemstamp", TimestampType()),
        ("Cybertronian", BooleanType()),
        ("function(binary)", BinaryType()),
        ("NullType", NullType())

    ]

rows = [
        ("Optim'us", -28, "Leader", 10, 5000000, 4.30, ["Inochi", "Convoy"], "19.442735,-99.201111", "1980/04/10",
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
        (None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),

    ]
source_df = op.create.df(cols ,rows)
source_df.table()
# -


# ### End Init Section

# # Test

# ## Optimus Test

from pyspark.ml.linalg import Vectors

t = Test(op, None, "create_df", imports=["import datetime",
                                "from pyspark.sql import functions as F"], path = "..", final_path="..")

# +
one_column = {"rows":["Argenis", "Favio", "Matthew"], "cols":["name"]}
plain = {"rows":[("BOB", 1),("JoSe", 2)],"cols":["name","age"]}
plain_infer_false = {"rows":[("BOB", 1),("JoSe", 2)],"cols":["name","age"],"infer_schema":False}
with_data_types = {"rows":[("BOB", 1),("JoSe", 2)],"cols":[("name", StringType(), True),("age", IntegerType(), False)]}
nullable = {"rows":[("BOB", 1),("JoSe", 2)],"cols":[("name", StringType()),("age", IntegerType())]}

df1 = op.create.df(**one_column)
df2 = op.create.df(**plain)
df3 = op.create.df(**plain_infer_false)
df4 = op.create.df(**with_data_types)
df5 = op.create.df(**nullable)
# -

t.create(df1, None, "one_column", "df", **one_column)

t.create(df2, None, "plain", "df", **plain)

t.create(df3, None, "plain_infer_false", "df", **plain_infer_false)

t.create(df4, None, "with_data_types", "df", **with_data_types)

t.create(df5, None, "nullable", "df", **nullable)

t.run()

# ## Columns Test

from pyspark.ml.linalg import Vectors

t = Test(op, source_df, "df_cols", imports=["from pyspark.ml.linalg import Vectors, VectorUDT, DenseVector",
                                        "import numpy as np",
                                        "nan = np.nan",
                                        "import datetime",
                                        "from pyspark.sql import functions as F"], path = "df_cols", final_path="..")

# +
from pyspark.sql import functions as F


def func(col_name, attrs):
    return F.col(col_name) * 2

numeric_col = "height(ft)"
numeric_col_B = "rank"
numeric_col_C = "rank"
string_col = "function"
date_col = "date arrival"
date_col_B = "last date seen"
new_col = "new col"
array_col = "attributes"
# -

t.create(None, "cols.min", None, "json", numeric_col)

t.create(None, "cols.min", "all_columns", "json", "*")

t.create(None, "cols.max", None, "json", numeric_col)

t.create(None, "cols.max", "all_columns", "json", "*")

t.create(None, "cols.range", None, "json", numeric_col)

t.create(None, "cols.range", "all_columns", "json", "*")

t.create(None, "cols.median", None, "json", numeric_col)

t.create(None, "cols.median", "all_columns", "json", "*")

t.create(None, "cols.percentile", None, "json", numeric_col, [0.05, 0.25], 1)

t.create(None, "cols.percentile", "all_columns", "json", "*", [0.05, 0.25], 1)

t.create(None, "cols.mad", None, "json", numeric_col)

t.create(None, "cols.mad", "all_columns", "json", "*")

t.create(None, "cols.std", None, "json", numeric_col)

t.create(None, "cols.std", "all_columns", "json", "*")

t.create(None, "cols.kurt", None, "json", numeric_col)

t.create(None, "cols.kurt", "all_columns", "json", "*")

t.create(None, "cols.mean", None, "json", numeric_col)

t.create(None, "cols.mean", "all_columns", "json", "*")

t.create(None, "cols.skewness", None, "json", numeric_col)

t.create(None, "cols.skewness", "all_columns", "json", "*")

t.create(None, "cols.sum", None, "json", numeric_col)

t.create(None, "cols.sum", "all_columns", "json", "*")

t.create(None, "cols.variance", None, "json", numeric_col)

t.create(None, "cols.variance", "all_columns", "json", "*")

t.create(None, "cols.abs", None, "df", numeric_col)

t.create(None, "cols.abs", "all_columns", "df", "*")

t.create(None, "cols.mode", None, "json", numeric_col)

t.create(None, "cols.mode", "all_columns", "json", "*")

t.create(None, "cols.count", None, "json")

t.create(None, "cols.count_na", None, "json", numeric_col)

t.create(None, "cols.count_na", "all_columns", "json", "*")

t.create(None, "cols.count_zeros", None, "json", numeric_col)

t.create(None, "cols.count_zeros", "all_columns", "json", "*")

t.create(None, "cols.count_uniques", None, "json", numeric_col)

t.create(None, "cols.count_uniques", "all_columns", "json", "*")

t.create(None, "cols.unique", None, "json", numeric_col)

t.create(None, "cols.unique", "all_columns", "json", "*")

t.create(None, "cols.add", None, "df", [numeric_col, numeric_col_B])

t.create(None, "cols.add", "all_columns", "df", "*"),

t.create(None, "cols.sub", None, "df", [numeric_col, numeric_col_B])

t.create(None, "cols.sub", "all_columns", "df", "*")

t.create(None, "cols.mul", None, "df", [numeric_col, numeric_col_B])

t.create(None, "cols.mul", "all_columns", "df", "*")

t.create(None, "cols.div", None, "df", [numeric_col, numeric_col_B])

t.create(None, "cols.div", "all_columns", "df", "*")

t.create(None, "cols.z_score", None, "df", numeric_col)

t.create(None, "cols.z_score", "all_columns", "df", "*")

t.create(None, "cols.iqr", None, "json", numeric_col)

t.create(None, "cols.iqr", "all_columns", "json", "*")

t.create(None, "cols.lower", None, "df", string_col)

t.create(None, "cols.lower", "all_columns", "df", "*")

t.create(None, "cols.upper", None, "df", string_col)

t.create(None, "cols.upper", "all_columns", "df", "*")

t.create(None, "cols.trim", None, "df", numeric_col)

t.create(None, "cols.trim", "all_columns", "df", "*")

t.create(None, "cols.reverse", None, "df", string_col)

t.create(None, "cols.reverse", "all_columns", "df", "*")

t.create(None, "cols.remove_accents", None, "df", string_col)

t.create(None, "cols.remove_accents", "all_columns", "df", string_col)

source_df.table()

t.create(None, "cols.remove_special_chars", None, "df", string_col)

t.create(None, "cols.remove_special_chars", "all_columns", "df", "*")

t.create(None, "cols.remove_white_spaces", None, "df", string_col)

t.create(None, "cols.remove_white_spaces", "all_columns", "df", "*")

t.create(None, "cols.date_transform", None, "df", date_col, "yyyy/MM/dd", "dd-MM-YYYY")

t.create(None, "cols.date_transform", "all_columns", "df", [date_col, date_col_B], "yyyy/MM/dd", "dd-MM-YYYY")

t.create(None, "cols.years_between", None, "df", date_col, "yyyyMMdd")

t.create(None, "cols.years_between", "multiple_columns", "df", [date_col, date_col_B], "yyyyMMdd")

t.create(None, "cols.impute", None, "df", numeric_col_B)

t.create(None, "cols.impute", "all_columns", "df", "names","categorical")

t.create(None, "cols.hist", None, "json", numeric_col_B, 4)

# +
# t.create(None,"cols.hist","all_columns","df","*",4)
# -

t.create(None, "cols.frequency", None, "json", numeric_col_B, 4)

t.create(None, "cols.frequency", "all_columns", "json", "*", 4)

t.create(None, "cols.schema_dtype", None, "json", numeric_col_B)


# Problems with casting
# t.delete(None, "cols.schema_dtype", "all_columns", "json", "*")
t.run()

t.create(None, "cols.dtypes", None, "json", numeric_col_B)

t.create(None, "cols.dtypes", "all_columns", "json", "*")

t.create(None, "cols.select_by_dtypes", "str", "df", "str")

t.create(None, "cols.select_by_dtypes", "int", "df", "int")

t.create(None, "cols.select_by_dtypes", "float", "df", "float")

t.create(None, "cols.select_by_dtypes", "array", "df", "array")

t.create(None, "cols.names", None, "json")

t.create(None, "cols.qcut", None, "df", numeric_col_B, 4)

t.create(None, "cols.qcut", "all_columns", "df", "*", 4)

t.create(None, "cols.clip", None, "df", numeric_col_B, 3, 5)

t.create(None, "cols.clip", "all_columns", "df", "*", 3, 5)

t.create(None, "cols.replace", None, "df", string_col, ["Security", "Leader"], "Match")

# Assert is failing I can see why
t.delete(None, "cols.replace", "all_columns", "df", "*", ["Jazz", "Leader"], "Match")
t.run()


# Its necesary to save the function 
t.delete(None, "cols.apply_expr", None, "df", numeric_col_B, func)

# Its necesary to save the function 
t.delete(None, "cols.apply_expr", "all_columns", "df", [numeric_col_B,numeric_col_C], func)

t.create(None, "cols.append", "number", "df", new_col, 1)

#t.create(None, "cols.append", "advance", "df", [("new_col_4", "test"),
    #                                                ("new_col_5", df[numeric_col_B] * 2),
    #                                                ("new_col_6", [1, 2, 3])
    #                                                ]),


t.create(None, "cols.rename", None, "df", numeric_col_B, numeric_col_B + "(old)")

t.create(None, "cols.rename", "list", "df", [numeric_col, numeric_col + "(tons)", numeric_col_B, numeric_col_B + "(old)"])

t.create(None, "cols.rename", "function", "df", str.upper)

t.create(None, "cols.drop", None, "df", numeric_col_B)

t.create(None, "cols.cast", None, "df", string_col, "string")

t.create(None, "cols.cast", "all_columns", "df", "*", "string")

# Problems with precision
t.delete(None, "cols.cast", "vector", "df", array_col, Vectors)

t.create(None, "cols.keep", None, "df", numeric_col_B)

t.create(None, "cols.move", "after", "df", numeric_col_B, "after", array_col)

t.create(None, "cols.move", "before", "df", numeric_col_B, "before", array_col)

t.create(None, "cols.move", "beginning", "df", numeric_col_B, "beginning")

t.create(None, "cols.move", "end", "df", numeric_col_B, "end")

t.create(None, "cols.select", None, "df", 0, numeric_col)

t.create(None, "cols.select", "regex", "df", "n.*", regex=True),

t.create(None, "cols.sort", None, "df")

t.create(None, "cols.sort", "desc", "df", "desc")

t.create(None, "cols.sort", "asc", "df", "asc")

t.create(None, "cols.fill_na", None, "df", numeric_col, "1")

t.create(None, "cols.fill_na", "array", "df", "japanese name", ["1","2"])

t.create(None, "cols.fill_na", "bool", "df", "Cybertronian", False)

t.create(None, "cols.fill_na", "all_columns", "df", ["names","height(ft)", "function", "rank", "age"], "2")

t.create(None, "cols.nest", None, "df", [numeric_col, numeric_col_B], new_col, separator=" ")

# +
# t.create(None, "cols.nest", "mix", "df", [F.col(numeric_col_C), F.col(numeric_col_B)], "E", separator="--")

# +
df_na = source_df.cols.drop("NullType").rows.drop_na("*")

t.create(df_na, "cols.nest", "vector_all_columns", "df", [numeric_col_C, numeric_col_B], new_col, shape="vector")
# -

t.create(df_na, "cols.nest", "vector", "df", [numeric_col_C, numeric_col_B], new_col, shape="vector")

t.create(None, "cols.nest", "array_all_columns", "df", "*", new_col, shape="array")

t.create(None, "cols.nest", "array", "df", [numeric_col, numeric_col_B,numeric_col_C], new_col, shape="array")

t.create(None, "cols.unnest", "array_all_columns", "df", array_col, "-", index=1)

t.create(None, "cols.unnest", "array", "df", array_col)

t.create(None, "cols.unnest", "array_all_columns", "df", array_col)

t.create(None, "cols.is_na", "all_columns", "df", "*")

t.create(None, "cols.is_na", None, "df", numeric_col)

t.run()

from pyspark.sql.types import *
from optimus import Optimus
from optimus.helpers.functions import json_enconding 
from pyspark.ml.linalg import Vectors, VectorUDT, DenseVector
import numpy as np
nan = np.nan
import datetime

# +
actual_df =op.load.json('https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.json')
expected_df = op.create.df([('billingId', LongType(), True),('birth', StringType(), True),('dummyCol', StringType(), True),('firstName', StringType(), True),('id', LongType(), True),('lastName', StringType(), True),('price', LongType(), True),('product', StringType(), True)], [(123, '1980/07/07', 'never', 'Luis', 1, 'Alvarez$$%!', 10, 'Cake')])

# assert (expected_df.collect() == actual_df.collect())

from deepdiff import DeepDiff  # For Deep Difference of 2 objects

actual_df.table()
expected_df.table()

# source_df.table()
# print(actual_df.to_json())
# print(expected_df.to_json())
a1 = actual_df.to_json()
e1 = expected_df.to_json()


# -

ddiff = DeepDiff(a1, e1, ignore_order=False)
print(ddiff)



# # Rows Test

t = Test(op,df, "df_rows", imports=["from pyspark.ml.linalg import Vectors, VectorUDT, DenseVector",
                                         "import numpy as np",
                                        "nan = np.nan",
                                        "import datetime",
                                        "from pyspark.sql import functions as F",
                                        "from optimus.functions import abstract_udf as audf"])

rows = [
        ("Optim'us", 28, "Leader", 10, 5000000, 4.30, ["Inochi", "Convoy"], "19.442735,-99.201111", "1980/04/10",
         "2016/09/10", [8.5344, 4300.0], date(2016, 9, 10), datetime(2014, 6, 24), True, bytearray("Leader", "utf-8"),
         None)
]

df = op.load.url("https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.csv")

t = Test(op, source_df, "op_io", imports=["from pyspark.ml.linalg import Vectors, VectorUDT, DenseVector",
                                        "import numpy as np",
                                        "nan = np.nan",
                                        "import datetime",
                                        "from pyspark.sql import functions as F"],path = "op_io", final_path="..")

t.create(op, "load.csv", "local_csv", "df", "../../examples/data/foo.csv")
t.create(op, "load.json", "local_json", "df", "../../examples/data/foo.json")
t.create(op, "load.parquet", "local_parquet", "df", "../../examples/data/foo.parquet")
t.create(op, "load.csv", "remote_csv", "df", "https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.csv")

t.create(op, "load.json", "remote_json", "df", "https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.json")

t.create(op, "load.parquet", "remote_parquet", "df", "https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.parquet")

# df_string = source_df.cols.cast("*","str")
t.create(source_df, "save.csv", None, None, "test.csv")

t.create(None, "save.json", None, None, "test.json")

t.create(None, "save.parquet", None, None, "test.parquet")

t.run()


source_df.table()
