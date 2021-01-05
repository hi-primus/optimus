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

from optimus import Optimus
op = Optimus("dask", n_workers=1, threads_per_worker=8, processes=False, memory_limit="3G", comm=True)

import numpy as np
import pandas as pd


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
    ("timestamp", TimestampType()),
    ("Cybertronian", BooleanType()),
    ("function(binary)", BinaryType()),
    ("NullType", NullType())

]

rows = [
    ("Optim'us", -28, "Leader", 10, 4000000, 4.30, ["Inochi", "Convoy"], "19.442735,-99.201111", "1980/04/10",
     "2016/09/10", [8.5344, 4300.0], date(2016, 9, 10), datetime(2014, 6, 24), True, bytearray("Leader", "utf-8"),
     None),
    ("bumbl#ebéé  ", 17, "Espionage", 7, 5000000, 2.0, ["Bumble", "Goldback"], "10.642707,-71.612534", "1980/04/10",
     "2015/08/10", [5.334, 2000.0], date(2015, 8, 10), datetime(2014, 6, 24), True, bytearray("Espionage", "utf-8"),
     None),
    ("ironhide&", 26, "Security", 7, 7000000, 4.0, ["Roadbuster"], "37.789563,-122.400356", "1980/04/10",
     "2014/07/10", [7.9248, 4000.0], date(2014, 6, 24), datetime(2014, 6, 24), True, bytearray("Security", "utf-8"),
     None),
    ("Jazz", 13, "First Lieutenant", 8, 5000000, 1.80, ["Meister"], "33.670666,-117.841553", "1980/04/10",
     "2013/06/10", [3.9624, 1800.0], date(2013, 6, 24), datetime(2014, 6, 24), True,
     bytearray("First Lieutenant", "utf-8"), None),
    ("Megatron", None, "None", 10, 5000000, 5.70, ["Megatron"], None, "1980/04/10", "2012/05/10", [None, 5700.0],
     date(2012, 5, 10), datetime(2014, 6, 24), True, bytearray("None", "utf-8"), None),
    ("Metroplex_)^$", 300, "Battle Station", 8, 5000000, None, ["Metroflex"], None, "1980/04/10", "2011/04/10",
     [91.44, None], date(2011, 4, 10), datetime(2014, 6, 24), True, bytearray("Battle Station", "utf-8"), None),
    (None, None, None, np.nan, None, None, None, None, None, None, None, None, None, None, None, None),

]

source_df = pd.DataFrame(columns = ['names','height(ft)','function',
                      'rank','age','weight(t)','japanese name','last position seen',
                      'date arrival','last date seen','attributes','Date Type','timestamp',
                      'Cybertronian','function(binary)','NullType'], 
             data= rows,
#              dtype = (object,object,object,
#                                 object,object,object,object,object,
#                                 object,object,object,object,object,
#                                 object,object)
            )

# from dask import dataframe as dd

# a=pd.DataFrame(source_df.to_dict())
from dask import dataframe as dd
source_df = dd.from_pandas(source_df, npartitions=1)


# -

source_df.compute()

# ### End Init Section

# # Test

# ## Columns Test

t = Test(op, source_df, "df_cols_dask", imports=["import numpy as np",
                                            "nan = np.nan",
                                            "import datetime",], path="df_cols_dask", final_path="..")

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

source_df.compute()

t.create(None, "cols.clip", "all_columns", "df", None,"*", 3, 5)

t.create(None, "cols.cast", "all_columns", "df", None, "*", "str")

t.create(None, "cols.is_na", "all_columns", "df", None, "*")

for i in source_df.compute().attributes:
    print(type(i[0]))

source_df.cols.unnest("attributes").compute()

source_df.compute()

source_df['height(ft)'].astype(str).str.split(".").compute()

import dask
source_df['height(ft)'].astype(str).str.split(".", expand=True, n=1)
# apply(dask.dataframe.Series, meta=("a")).compute()

source_df['attributes'].compute().apply(pd.Series)

source_df = source_df.repartition(6)

source_df["date arrival"].astype(str).str.split("/", expand=True, n=2).compute()

source_df.compute()
