# Reference http://nbviewer.jupyter.org/github/julioasotodv/spark-df-profiling/blob/master/examples/Demo.ipynb

import pyspark.sql.functions as F
from optimus.helpers.functions import *


class Profiler:
    def __init__(self, df):
        self._df = df

    def column(self, columns):
        df = self._df

        columns = parse_columns(df, columns)

        print(len(df.columns))
        print(df.count())

        print(df.cols().count_na(columns))

        print(df.cols().count_zeros(columns))

        print(df.cols().count_uniques(columns))

        print(df.dtypes)

        return
