# Reference http://nbviewer.jupyter.org/github/julioasotodv/spark-df-profiling/blob/master/examples/Demo.ipynb

import pyspark.sql.functions as F


class Profiler:
    def __init__(self, df):
        self._df = df
        pass

    def column(self):
        df = self._df
        print(len(df.columns))
        print(df.count())

        print(df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in df.columns]).show())

        df.cols().count("*")



        print(df.dtypes)

        print([df.distinct(c).count() for c in df.columns])

        return
