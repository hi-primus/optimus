# This functions must handle one or multiple columns
# Must return None if the data type can not be handle
import numpy as np
import pandas as pd

# from fast_histogram import histogram1d
from optimus.engines.jit import numba_histogram

# DataFrame = pd.DataFrame
#
#
# def functions(self):
#     class Functions:
#
#         # def hist_agg(col_name, df, buckets, min_max=None, dtype=None):
#         @staticmethod
#         def hist_agg(columns, args, df):
#
#             return False
#
#     return Functions()
#
#
# DataFrame.functions = property(functions)
