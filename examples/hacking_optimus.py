# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
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

# ## The instruction bellow automagically reload Optimus if you made any modification on the library

# If you modify Optimus or any library this code is going to reload it
# %load_ext autoreload
# %autoreload 

# If you are in the example folder. This is the way to find optimus
import sys
sys.path.append("..")

# Create Optimus
from optimus import Optimus
op = Optimus()

# # Go to optimus/create.py. In 'def data_frame()' write print("Hello World"). Now run the cell below and you should see "Hello World" bellow the cell

# +

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

df = op.create.df(
            [
                ("words", "str", True),
                ("num", "int", True),
                ("animals", "str", True),
                ("thing", StringType(), True),
                ("two strings", StringType(), True),
                ("filter", StringType(), True),
                ("num 2", "string", True),
                ("col_array",  ArrayType(StringType()), True),
                ("col_int",  ArrayType(IntegerType()), True)

            ]
,
[
                ("  I like     fish  ", 1, "dog", "housé", "cat-car", "a","1",["baby", "sorry"],[1,2,3]),
                ("    zombies", 2, "cat", "tv", "dog-tv", "b","2",["baby 1", "sorry 1"],[3,4]),
                ("simpsons   cat lady", 2, "frog", "table","eagle-tv-plus","1","3", ["baby 2", "sorry 2"], [5,6,7]),
                (None, 3, "eagle", "glass", "lion-pc", "c","4", ["baby 3", "sorry 3"] ,[7,8])
            ])

#df.table()
