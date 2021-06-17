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
#   kernel_info:
#     name: python3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # This notebook create the tests in python code. All this cells must be run to executed the tests

# %load_ext autoreload
# %autoreload 2

# + {"outputHidden": false, "inputHidden": false}
import sys
sys.path.append("../..")
# -

from optimus import Optimus
from optimus.helpers.test import Test

op = Optimus("cudf", n_workers=1, threads_per_worker=8, processes=False, memory_limit="3G", comm=True)

# +
import pandas as pd
from pyspark.sql.types import *
from datetime import date, datetime

cols = [
    "names",
    "height(ft)", 
    "function", 
    "rank", 
    "age",
    "weight(t)",
    "japanese name",
    "last position seen",
    "date arrival",
    "last date seen",
    "attributes",
    "Date Type",
    "timestamp",
    "Cybertronian",
    "function(binary)",
    "NullType",
    "Mixed"
]

   
        
rows = [
    ("Optim'us", -28, "Leader", 10, 4000000, 4.30, ["Inochi", "Convoy"], "19.442735,-99.201111", "1980/04/10",
     "2016/09/10", [8.5344, 4300.0], date(2016, 9, 10), datetime(2014, 6, 24), True, bytearray("Leader", "utf-8"),
     None, "male"),
    ("bumbl#ebéé  ", 17, "Espionage", 7, 5000000, 2.0, ["Bumble", "Goldback"], "10.642707,-71.612534", "1980/04/10",
     "2015/08/10", [5.334, 2000.0], date(2015, 8, 10), datetime(2014, 6, 24), True, bytearray("Espionage", "utf-8"),
     None, "1.1.1.1" ),
    ("ironhide&", 26, "Security", 7, 7000000, 4.0, ["Roadbuster"], "37.789563,-122.400356", "1980/04/10",
     "2014/07/10", [7.9248, 4000.0], date(2014, 6, 24), datetime(2014, 6, 24), True, bytearray("Security", "utf-8"),
     None,"http://hi-optimus.com"),
    ("Jazz", 13, "First Lieutenant", 8, 5000000, 1.80, ["Meister"], "33.670666,-117.841553", "1980/04/10",
     "2013/06/10", [3.9624, 1800.0], date(2013, 6, 24), datetime(2014, 6, 24), True,
     bytearray("First Lieutenant", "utf-8"), None,"5123456789123456"),
    ("Megatron", None, "None", 10, 5000000, 5.70, ["Megatron"], None, "1980/04/10", "2012/05/10", [None, 5700.0],
     date(2012, 5, 10), datetime(2014, 6, 24), True, bytearray("None", "utf-8"), None, "11529"),
    ("Metroplex_)^$", 300, "Battle Station", 8, 5000000, None, ["Metroflex"], None, "1980/04/10", "2011/04/10",
     [91.44, None], date(2011, 4, 10), datetime(2014, 6, 24), True, bytearray("Battle Station", "utf-8"), None, "3"),
    (None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "optimus"),

]

source_df = pd.DataFrame(columns = cols, 
             data= rows)

from dask import dataframe as dd
source_df = dd.from_pandas(source_df, npartitions=1)


# -

source_df.cols.dtypes()

# +
cols = [('new_col', str),('new_col_2',str)]
rows = [(1,2),(2,3),(3,3),(4,3),(5,3),(6,3),(7,3)]

# df_col = dd.from_pandas(pd.DataFrame(columns =cols , data= [(1,2),(2,3),(3,3),(4,3),(5,3),(6,3),(7,3)], dtype=dict(zip(cols,[int, str]))), npartitions=1)
df_col = op.create.df(cols, rows)
print(df_col)                         
# -

# ### End Init Section

# # Test

# ## Columns Test

t = Test(op, source_df, "df_cols_dask", imports=["import numpy as np",
                                            "nan = np.nan",
                                            "import datetime",], path="df_cols_dask", final_path="..")

# +


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

print(numeric_col_B)
t.create(None, "cols.impute", None, "df", None, numeric_col_B)
t.run()

# +
# op.create.df()
# -

source_df.cols.select("rank")

t.create(None, "cols.hist", None, "json", None, ["height(ft)", numeric_col_B], 4)

t.create(source_df, "cols.string_to_index", None, "df", None, "rank")

# +
# FIX at creation time we los the metadata. Need to find a way to put it on the dataframe creation
# t.create(source_df, "cols.index_to_string", None, "df", None, "rank***STRING_TO_INDEX")
# -

t.run()

t.create(None, "cols.remove", None, "df", None, string_col, "i")

t.run()

t.create(None, "cols.remove", "list", "df", None, string_col, ["a", "i", "Es"])

t.create(None, "cols.remove", "list_output", "df", None, string_col, ["a", "i", "Es"], output_cols=string_col + "_new")

t.run()

t.create(None, "cols.min", None, "json", None, numeric_col)

source_df.cols.names()

t.create(None, "cols.min", "all_columns", "json", None, "*")
t.run()

t.create(None, "cols.max", None, "json", None, numeric_col)

t.create(None, "cols.max", "all_columns", "json", None, "*")
t.run()

source_df.cols.min("rank")

t.create(None, "cols.range", None, "json", None, numeric_col)

source_df.display()

t.create(None, "cols.range", "all_columns", "json", None, "*")

t.run()

t.create(None, "cols.median", None, "json", None, numeric_col)

t.create(None, "cols.median", "all_columns", "json", None, "*")

t.run()

t.create(None, "cols.percentile", None, "json", None, numeric_col, [0.05, 0.25], 1)

t.create(None, "cols.percentile", "all_columns", "json", None, "*", [0.05, 0.25], 1)

# ## MAD

# +

source_df.cols.mad(numeric_col, True)
# -

t.create(None, "cols.mad", None, "json", None, numeric_col)

t.create(None, "cols.mad", "all_columns", "json", None, "*")

t.run()

t.create(None, "cols.std", None, "json", None, numeric_col)

t.create(None, "cols.std", "all_columns", "json", None, "*")
t.run()

t.create(None, "cols.kurt", None, "json", None, numeric_col)
t.run()

t.create(None, "cols.kurt", "all_columns", "json", None, "*")
t.run()

t.create(None, "cols.mean", None, "json",None, numeric_col)

t.create(None, "cols.mean", "all_columns", "json", None, "*")
t.run()

t.create(None, "cols.skewness", None, "json", None, numeric_col)

t.create(None, "cols.skewness", "all_columns", "json", None, "*")
t.run()

t.create(None, "cols.sum", None, "json", None,numeric_col)

t.create(None, "cols.sum", "all_columns", "json", None, "*")
t.run()

t.create(None, "cols.variance", None, "json",None, numeric_col)

t.create(None, "cols.variance", "all_columns", "json", None, "*")
t.run()

source_df.table()

t.create(None, "cols.abs", None, "df", None, "weight(t)")
t.run()

t.create(None, "cols.abs", "all_columns", "df", None, "*")

source_df.table()

source_df.mode(dropna=True)

t.create(None, "cols.mode", None, "json", None, numeric_col)

# %%time
t.create(None, "cols.mode", "all_columns", "json", None, "*")

t.run()

t.create(None, "cols.count", None, "json")

# ## Count na

t.create(None, "cols.count_na", None, "json", None, numeric_col)

t.create(None, "cols.count_na", "all_columns", "json", None, "*")

t.run()

t.create(None, "cols.count_zeros", None, "json", None, numeric_col)

source_df.compute()

t.create(None, "cols.count_zeros", "all_columns", "json", None, "*")
t.run()

source_df.cols.names()

# ## Value counts

t.create(None, "cols.value_counts", None, "json", None, numeric_col)
t.run()

t.create(None, "cols.value_counts", "all_columns", "json", None, "*")
t.run()

t.create(None, "cols.count_uniques", None, "json", None, numeric_col)
t.run()

t.create(None, "cols.count_uniques", "all_columns", "json", None, "*")
t.run()

t.create(None, "cols.unique", None, "df", None, numeric_col)
t.run()

t.create(None, "cols.unique", "all_columns", "df", None, "*")
t.run()

t.create(None, "cols.add", None, "df", None, [numeric_col, numeric_col_B])

t.create(None, "cols.add", "all_columns", "df", None, "*"),

t.create(None, "cols.sub", None, "df", None,[numeric_col, numeric_col_B])

t.create(None, "cols.sub", "all_columns", "df",None, "*")

t.create(None, "cols.mul", None, "df", None,[numeric_col, numeric_col_B])

t.create(None, "cols.mul", "all_columns", "df", None, "*")

t.create(None, "cols.div", None, "df", None,[numeric_col, numeric_col_B])

t.create(None, "cols.div", "all_columns", "df", None,"*")

t.create(None, "cols.z_score", None, "df", None,numeric_col)

source_df.compute()

t.create(None, "cols.z_score", "all_columns", "df", None, "*")

t.create(None, "cols.iqr", None, "json", None, None, numeric_col)

t.create(None, "cols.iqr", "all_columns", "json", None, "*")

t.run()

source_df.display()

t.create(None, "cols.lower", None, "df", None, string_col)

t.create(None, "cols.lower", "all_columns", "df", None, "*")

t.create(None, "cols.upper", None, "df", None, string_col)

t.create(None, "cols.upper", "all_columns", "df", None, "*")

t.create(None, "cols.trim", None, "df", None, string_col)

t.create(None, "cols.trim", "all_columns", "df", None, "*")

print(string_col)
t.create(None, "cols.reverse", None, "df", None, string_col)

t.create(None, "cols.reverse", "all_columns", "df", None, "*")

source_df.compute()

print(string_col)
t.create(None, "cols.remove_accents", None, "df", None, string_col)

t.create(None, "cols.remove_accents", "all_columns", "df", None,string_col)

source_df.table()

t.create(None, "cols.remove_special_chars", None, "df", None, string_col)

t.create(None, "cols.remove_special_chars", "all_columns", "df", None, "*")
t.run()
# t.create(None, "cols.value_counts", None, "json", None, numeric_col)

t.create(None, "cols.remove_white_spaces", None, "df", None, string_col)

t.create(None, "cols.remove_white_spaces", "all_columns", "df", None,"*")

print(date_col)
t.create(None, "cols.date_format", None, "df", None,date_col, "%Y/%m/%d", "%d/%m/%Y")

t.run()

t.create(None, "cols.date_format", "all_columns", "df", None, [date_col, date_col_B], "%Y/%m/%d", "%d/%m/%Y")

# t.create(None, "cols.years_between", None, "df", date_col, "yyyy/MM/dd")
t.create(None, "cols.years_between", None, "df", None, date_col, "%Y/%m/%d")

# t.create(None, "cols.years_between", "multiple_columns", "df", [date_col, date_col_B], "yyyy/MM/dd")
t.create(None, "cols.years_between", "multiple_columns", "df", None,[date_col, date_col_B], "%Y/%m/%d")

t.run()

print(numeric_col_B)
t.create(None, "cols.impute", None, "df", None, numeric_col_B)

source_df.compute()

# %%time
t.create(None, "cols.impute", "multiple_columns", "df", None, "names","categorical")


t.run()

# ## Hist

t.create(None, "cols.hist", None, "json", None, ["height(ft)", numeric_col_B], 4)
t.run() 

t.create(None, "cols.hist", "all_columns", "json", None, "*", 4)

t.run()

t.create(None, "cols.frequency", None, "dict", None, numeric_col_B, 4)
t.run()

t.create(None, "cols.frequency", "all_columns", "dict", None, "*", 4)
t.run()

t.create(None, "cols.schema_dtype", None, "json", None, numeric_col_B)

# Problems with casting
# t.delete(None, "cols.schema_dtype", "all_columns", "json", "*")
t.run()

t.create(None, "cols.dtypes", None, "json", None, numeric_col_B)

t.run()

t.create(None, "cols.dtypes", "all_columns", "json",None, "*")

t.create(None, "cols.names", None, "json")

source_df.compute()

print(numeric_col_B)
# t.create(None, "cols.qcut", None, "df",None, numeric_col_B, 4)

# +
# t.create(None, "cols.qcut", "all_columns", "df", None,"*", 4)
# -

t.create(None, "cols.clip", None, "df", None,numeric_col_B, 3, 5)

source_df.dtypes

t.create(None, "cols.clip", "all_columns", "df", None,"*", 3, 5)

source_df["names"].str.replace(r'\d+', '').compute()

print(string_col)
t.create(None, "cols.remove_numbers", None, "df", None, string_col)

t.create(None, "cols.replace", "full", "df", None, string_col, ["First Lieutenant", "Battle"], "Match",
         search_by="full")

t.create(None, "cols.replace", "words", "df", None, string_col, ["Security", "Leader"], "Match", search_by="words")
t.run()

t.create(None, "cols.replace", "chars", "df", None, string_col, ["F", "E"], "Match", search_by="chars")

# +
# t.create(None, "cols.replace", "numeric", "df", None, "age", 5000000, 5, search_by="numeric")
# -

t.run()

# Assert is failing I can see why
t.create(None, "cols.replace", "all_columns", "df", None, "*", ["Jazz", "Leader"], "Match")
t.run()

# +
# Its necesary to save the function
# t.delete(None, "cols.apply_expr", None, "df", numeric_col_B, func)

# +
# Its necesary to save the function
# t.delete(None, "cols.apply_expr", "all_columns", "df", [numeric_col_B, numeric_col_C], func)

# +
# t.create(None, "cols.append", "number", "df", None, new_col, 1)
# -

df_col.to_json()

t.delete(None, "cols.append", "dataframes", "df", None, df_col)
t.run()

# +
# t.delete(None, "cols.append", "advance", "df", [("new_col_4", "test"),
#                                                ("new_col_5", df[numeric_col_B] * 2),
#                                                ("new_col_6", [1, 2, 3])
#                                                ]),
# -


t.create(None, "cols.rename", None, "df", None, numeric_col_B, numeric_col_B + "(old)")

t.create(None, "cols.rename", "list", "df",None,
         [numeric_col, numeric_col + "(tons)", numeric_col_B, numeric_col_B + "(old)"])

t.create(None, "cols.rename", "function", "df", None, str.upper)

print(numeric_col_B)
t.create(None, "cols.drop", None, "df", None,numeric_col_B)

t.create(None, "cols.cast", None, "df", None, string_col, "string")

t.create(None, "cols.cast", "all_columns", "df", None, "*", "str")

t.run()

# +
# Problems with precision
# t.delete(None, "cols.cast", "vector", "df", array_col, Vectors)
# -

source_df.cols.keep(numeric_col_B).display()

t.create(None, "cols.keep", None, "df", None, numeric_col_B)

t.create(None, "cols.move", "after", "df",None, numeric_col_B, "after", array_col)

t.create(None, "cols.move", "before", "df",None, numeric_col_B, "before", array_col)

t.create(None, "cols.move", "beginning", "df", None, numeric_col_B, "beginning")

t.create(None, "cols.move", "end", "df", None, numeric_col_B, "end")

t.create(None, "cols.select", None, "df", None, 0, numeric_col)

t.create(None, "cols.select", "regex", "df", None, "n.*", regex=True),

t.create(None, "cols.sort", None, "df")
t.run()

t.create(None, "cols.sort", "desc", "df", None, "desc")

t.create(None, "cols.sort", "asc", "df", None, "asc")

t.run()

t.create(None, "cols.fill_na", None, "df", None, numeric_col, "1")

# +
# t.create(None, "cols.fill_na", "array", "df", None, "japanese name", ["1", "2"])
# -

t.run()

# t.create(None, "cols.fill_na", "bool", "df", None, "Cybertronian", False)
t.create(None, "cols.fill_na", None, "df", None, "Cybertronian", False)

t.run()

# + {"jupyter": {"outputs_hidden": true}}
t.create(None, "cols.fill_na", "all_columns", "df", None,["names", "height(ft)", "function", "rank", "age"], "2")
# -

# ## Nest

t.create(None, "cols.nest", None, "df", None, [numeric_col, numeric_col_B], separator=" ", output_col=new_col)

# +
# t.create(None, "cols.nest", "mix", "df", [F.col(numeric_col_C), F.col(numeric_col_B)], "E", separator="--")
# -

df_na = source_df.cols.drop("NullType").rows.drop_na("*")

# +


t.create(df_na, "cols.nest", "vector_all_columns", "df", None, [numeric_col_C, numeric_col_B], shape="vector",
         output_col=new_col)
# -

t.create(df_na, "cols.nest", "vector", "df", None, [numeric_col_C, numeric_col_B], shape="vector", output_col=new_col)

t.create(None, "cols.nest", "array", "df", None, [numeric_col, numeric_col_B, numeric_col_C], shape="array",
         output_col=new_col)

t.run()

# +
# import pandas as pd
# from pyspark.sql.types import *
# from datetime import date, datetime

# cols = [
#     ("col 1", "str"),
#      ("col 2", "str"),
#      ("col 3", "str"),
# ]

# rows = [
#      ("male", "male", 1),
#         ("optimus", "bumblebee", 1),
#         ("3", "4.1", 1),
#         ("true", "False", 1),
#         ("[1,2,3,4]", "(1,2,3,4)", 1),
#         ("{1,2,3,4}", "{'key1' :1 , 'key2':2}", 1),
#         ("1.1.1.1", "123.123.123.123", 1),
#         ("http://hi-optimuse.com", "https://hi-bumblebee.com", 1),
#         ("optimus@cybertron.com", "bumblebee@cybertron.com", 1),
#         ("5123456789123456", "373655783158306", 1),
#         ("11529", "30345", 1),
#         ("04/10/1980", "04/10/1980", 1),
#         ("null", "Null", 1),
#         ("", "", 1),
#         (None, None, 1)

# ]

# source_df = pd.DataFrame(columns = cols, 
#              data= rows)

# # from dask import dataframe as dd

# # a=pd.DataFrame(source_df.to_dict())
# from dask import dataframe as dd
# source_df = dd.from_pandas(source_df, npartitions=1)
# -

mismatch = {"names": "dd/mm/yyyy", "height(ft)": r'^([0-2][0-9]|(3)[0-1])(\/)(((0)[0-9])|((1)[0-2]))(\/)\d{4}$',
            "function": "yyyy-mm-dd"}

t.create(None, "cols.count_mismatch", "int", "json", None, {"Mixed": "int"})
t.run()

t.create(None, "cols.count_mismatch", "decimal", "json", None, {"Mixed": "decimal"})
t.run()

t.create(None, "cols.count_mismatch", "email", "json", None, {"Mixed": "email"})

t.create(None, "cols.count_mismatch", "url", "json", None, {"Mixed": "url"})

t.create(None, "cols.count_mismatch", "ip", "json", None, {"Mixed": "ip"})

t.create(None, "cols.count_mismatch", "boolean", "json", None, {"Mixed": "boolean"})

t.create(None, "cols.count_mismatch", "credit_card", "json", None, {"Mixed": "credit_card_number"})

t.create(None, "cols.count_mismatch", "object", "json", None, {"Mixed": "object"})

t.create(None, "cols.count_mismatch", "array", "json", None, {"Mixed": "array"})

t.create(None, "cols.count_mismatch", "date", "json", None, {"Mixed": "date"})

t.create(None, "cols.count_mismatch", "zip_code", "json", None, {"Mixed": "zip_code"})

t.create(None, "cols.count_mismatch", "string", "json", None, {"Mixed": "string"})

t.run()

# ## Unnest String

source_df.cols.select(3).compute()

source_df.columns

t.create(None, "cols.unnest", "string_index", "df", None, date_col, "/", splits=3)

t.create(None, "cols.unnest", "string_multi_index", "df", None, date_col, "/", splits=3, index=[1, 2])

t.create(None, "cols.unnest", "string_infer_split", "df", None, date_col, "/")

t.create(None, "cols.unnest", "string_no_index", "df", None, date_col, "/", splits=3)

t.create(None, "cols.unnest", "string_output_columns", "df", None, date_col, "/", splits=3,
         output_cols=["year", "month", "day"])

t.create(None, "cols.unnest", "array_index", "df", None, array_col, index=1, mode="array")

t.create(None, "cols.unnest", "array_multi_index", "df", None, array_col, index=[0, 1], mode="array")

t.run()

t.create(None, "cols.unnest", "string_multi_colum_multi_index_multi_output", "df", None,
         ["date arrival", "last date seen"], "/", index=[[0, 1], [0, 1]],
         output_cols=[("year1", "month1"), ("year2", "month2")])

t.create(None, "cols.unnest", "string_multi_colum_multi_output", "df", None, ["date arrival", "last date seen"], "/",
         output_cols=[("year1", "month1"), ("year2", "month2")])

t.create(None, "cols.unnest", "array", "df", None,array_col, mode="array")

t.create(None, "cols.unnest", "array_all_columns", "df", None,array_col, mode="array")

t.create(None, "cols.is_na", "all_columns", "df", None, "*")

t.create(None, "cols.is_na", None, "df", None, numeric_col)

t.run()

# +

import numpy as np
# -

nan = np.nan
import datetime

# +
actual_df = op.load.json('https://raw.githubusercontent.com/hi-primus/optimus/master/examples/data/foo.json', multiline=True)

# expected_df = op.create.df(
#     [('billingId', LongType(), True), ('birth', StringType(), True), ('dummyCol', StringType(), True),
#      ('firstName', StringType(), True), ('id', LongType(), True), ('lastName', StringType(), True),
#      ('price', LongType(), True), ('product', StringType(), True)],
#     [(123, '1980/07/07', 'never', 'Luis', 1, 'Alvarez$$%!', 10, 'Cake')])

# # assert (expected_df.collect() == actual_df.collect())

# from deepdiff import DeepDiff  # For Deep Difference of 2 objects

# actual_df.table()
# expected_df.table()

# a1 = actual_df.to_json()
# e1 = expected_df.to_json()


# +
# ddiff = DeepDiff(a1, e1, ignore_order=False)
# print(ddiff)
# -

# # Rows Test

t = Test(op, df, "df_rows", imports=["import numpy as np",
                                     "nan = np.nan",
                                     "import datetime"])

rows = [
    ("Optim'us", 28, "Leader", 10, 5000000, 4.30, ["Inochi", "Convoy"], "19.442735,-99.201111", "1980/04/10",
     "2016/09/10", [8.5344, 4300.0], date(2016, 9, 10), datetime(2014, 6, 24), True, bytearray("Leader", "utf-8"),
     None)
]

df = op.load.csv("https://raw.githubusercontent.com/hi-primus/optimus/master/examples/data/foo.csv")

t = Test(op, source_df, "op_io_dask", imports=["import numpy as np",
                                          "nan = np.nan",
                                          "import datetime",], path="op_io_dask", final_path="..")

t.create(op, "load.csv", "local_csv", "df", None, "../../examples/data/foo.csv")
t.create(op, "load.json", "local_json", "df",None,"../../examples/data/foo.json", multiline=True)
# t.create(op, "load.parquet", "local_parquet", "df", None, "../../examples/data/foo.parquet")
t.create(op, "load.csv", "remote_csv", "df", None,
         "https://raw.githubusercontent.com/hi-primus/optimus/master/examples/data/foo.csv")

t.create(op, "load.json", "remote_json", "df", None,
         "https://raw.githubusercontent.com/hi-primus/optimus/master/examples/data/foo.json", multiline= True)

# +
# # !pip install pyarrow

# +
# t.create(op, "load.parquet", "remote_parquet", "df", None,
#          "https://raw.githubusercontent.com/hi-primus/optimus/master/examples/data/foo.parquet")

# +
from optimus.profiler.profiler import Profiler

p = Profiler()

print(p.run(source_df1, "japanese name"))
# -

# df_string = source_df.cols.cast("*","str")
t.create(source_df, "save.csv", None, None, None, "test.csv")

t.create(None, "save.json", None, None, None, "test.json")

# +
# t.create(None, "save.parquet", None, None, None, "test.parquet")
# -

t.run()

source_df.table()

# # Ouliers

t = Test(op, source_df, "df_outliers_dask", imports=["import numpy as np",
                                                "nan = np.nan",
                                                "import datetime"], path="df_outliers_dask",final_path="..")

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

source_df.table()

# ## Tukey

t.create(None, "outliers.tukey", None, "df", "select", numeric_col)

source_df.outliers.tukey(numeric_col).drop().table()

t.create(None, "outliers.tukey", None, "df", "drop", numeric_col)

t.create(None, "outliers.tukey", None, "json", "whiskers", numeric_col)

t.create(None, "outliers.tukey", None, "json", "count", numeric_col)

t.create(None, "outliers.tukey", None, "json", "non_outliers_count", numeric_col)

t.create(None, "outliers.tukey", None, "json", "info", numeric_col)

t.run()

# ## Zscore

threshold = 0.5

t.create(None, "outliers.z_score", None, "df", "select", numeric_col, threshold)

source_df.outliers.z_score('height(ft)', 0.5).select()

t.create(None, "outliers.z_score", None, "df", "drop", numeric_col, threshold)

t.create(None, "outliers.z_score", None, "json", "count", numeric_col, threshold)

t.create(None, "outliers.z_score", None, "json", "non_outliers_count", numeric_col, threshold)

t.create(None, "outliers.z_score", None, "json", "info", numeric_col, threshold)

t.run()

source_df.cols.mad(numeric_col)

# ## Modified Zscore

threshold = 0.5
relative_error = 10000

t.create(None, "outliers.modified_z_score", None, "df", "select", numeric_col, threshold, relative_error)

t.create(None, "outliers.modified_z_score", None, "df", "drop", numeric_col, threshold, relative_error)

t.create(None, "outliers.modified_z_score", None, "json", "count", numeric_col, threshold, relative_error)

t.create(None, "outliers.modified_z_score", None, "json", "non_outliers_count", numeric_col, threshold, relative_error)

t.create(None, "outliers.modified_z_score", None, "json", "info", numeric_col, threshold, relative_error)

t.run()

# ## Mad

threshold = 0.5
relative_error = 10000

t.create(None, "outliers.mad", None, "df", "select", numeric_col, threshold, relative_error)

t.create(None, "outliers.mad", None, "df", "drop", numeric_col, threshold, relative_error)

t.create(None, "outliers.mad", None, "json", "count", numeric_col, threshold, relative_error)

t.create(None, "outliers.mad", None, "json", "non_outliers_count", numeric_col, threshold, relative_error)

t.create(None, "outliers.mad", None, "json", "info", numeric_col, threshold, relative_error)

t.run()

# ## Keycolision

source_df = op.load.csv("../../examples/data/random.csv", header=True, sep=";", dtype={'ADDRESS2': 'object',
       'COCINA': 'object',
       'CONDO': 'object',
       'ENGANCHE': 'object',
       'EXCEDENTE': 'float64',
       'FECHA_Entregado': 'object',
       'FECHA_Liberado': 'object',
       'FechaVencimientoApartado': 'object',
       'LT': 'object',
       'MZ': 'object',
       'NIVEL': 'object',
       'NOUNI': 'object',
       'OTRO2': 'object',
       'PAGINI': 'object',
       'cCodigoPostal': 'object'})

source_df.table()

# +
t = Test(op, source_df, "df_keycollision_dask", imports=["import numpy as np",
                                                    "nan = np.nan",
                                                    "import datetime",],
         path="df_keycollision_dask", final_path="..")


# -

from optimus.engines.pandas.ml import keycollision as keyCol

# +
# def func(values, ele_func, args):
#     return values.apply(ele_func, args)

# def ele_func(values, args):
#     return values

# args= ""
# source_df.map_partitions(func, ele_func, args)

# + {"outputHidden": false, "inputHidden": false}
t.create(keyCol, "fingerprint", None, "df", None, source_df, "STATE")
t.run()

# + {"outputHidden": false, "inputHidden": false}
t.create(keyCol, "fingerprint_cluster", None, "json", None, source_df, "STATE")
# -

t.run()

# + {"outputHidden": false, "inputHidden": false}
t.create(keyCol, "n_gram_fingerprint", None, "df", None, source_df, "STATE")

# + {"outputHidden": false, "inputHidden": false}
t.create(keyCol, "n_gram_fingerprint_cluster", None, "json", None, source_df, "STATE", 2)

# + {"outputHidden": false, "inputHidden": false}
t.run()
# -

# ## Distance cluster

source_df = op.read.csv("../../examples/data/random.csv", header=True, sep=";").limit(1000)

# + {"outputHidden": false, "inputHidden": false}
t = Test(op, source_df, "df_distance_cluster", imports=[\
                                                        "import numpy as np",
                                                        "nan = np.nan",
                                                        "import datetime",
                                                        "from pyspark.sql import functions as F",
                                                        "from optimus.ml import distancecluster as dc"],
         path="df_distance_cluster", final_path="..")

from optimus.ml import distancecluster as dc

# -

df.table()

# + {"outputHidden": false, "inputHidden": false}
t.create(dc, "levenshtein_cluster", None, 'dict', None, source_df, "STATE")
# -

t.run()

df_cancer = op.spark.read.csv('../data_cancer.csv', sep=',', header=True, inferSchema=True)

columns = ['diagnosis', 'radius_mean', 'texture_mean', 'perimeter_mean', 'area_mean', 'smoothness_mean',
           'compactness_mean', 'concavity_mean', 'concave points_mean', 'symmetry_mean',
           'fractal_dimension_mean']

df_model.table()

# ## Row

source_df = op.create.df([
    ("words", "str", True),
    ("num", "int", True),
    ("animals", "str", True),
    ("thing", StringType(), True),
    ("second", "int", True),
    ("filter", StringType(), True)
],
    [
        ("  I like     fish  ", 1, "dog dog", "housé", 5, "a"),
        ("    zombies", 2, "cat", "tv", 6, "b"),
        ("simpsons   cat lady", 2, "frog", "table", 7, "1"),
        (None, 3, "eagle", "glass", 8, "c"),
    ])

from optimus.engines.spark.audf import abstract_udf as audf

t = Test(op, source_df, "df_rows_dask", imports=["import numpy as np",
                                            "nan = np.nan",                                            
                                            "import datetime",], path="df_rows_dask", final_path="..")

row = [("this is a word", 2, "this is an animal",
        "this is a thing", 64, "this is a filter",)]

t.create(None, "rows.append", None, "df", None, row)

fil = (source_df["num"] == 1)

t.create(None, "rows.select", None, "df", None, fil)

t.create(None, "rows.select_by_dtypes", None, "df", None, "filter", "integer")

fil = (source_df["num"] == 2) | (source_df["second"] == 5)
print(str(fil))
# type(fil)

t.create(None, "rows.drop", None, "df", None, fil)

t.create(None, "rows.drop_by_dtypes", None, "df", None, "filter", "integer")


def func_data_type(value, attr):
    return value > 1


a = audf("num", func_data_type, "boolean")

t.create(None, "rows.drop", "audf", "df", None, a)

t.create(None, "rows.sort", None, "df", None, "num", "desc")

t.create(None, "rows.is_in", None, "df", None, "num", 2)

t.create(None, "rows.between", None, "df", None, "second", 6, 8)

t.create(None, "rows.between", "equal", "df", None, "second", 6, 8, equal=True)

t.create(None, "rows.between", "invert_equal", "df", None, "second", 6, 8, invert=True, equal=True)

t.create(None, "rows.between", "bounds", "df", None, "second", bounds=[(6, 7), (7, 8)], invert=True, equal=True)

t.run()

