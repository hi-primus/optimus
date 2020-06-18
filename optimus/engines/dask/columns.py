import dask.dataframe as dd
import pandas as pd
from dask import delayed
from dask.dataframe.core import DataFrame

from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.columns import parse_columns


# This implementation works for Dask
def cols(self: DataFrame):
    class Cols(DaskBaseColumns):
        def __init__(self, df):
            super(DaskBaseColumns, self).__init__(df)

        @staticmethod
        def min(columns, skip_na=True):
            """
            This calculate the min for a columns.
            :param skip_na:
            :param columns:
            :return:
            """

            columns = parse_columns(self, columns)
            df = self[columns]
            partitions = df.to_delayed()

            @delayed
            def _min(pdf):
                # Using numpy min is faster https://stackoverflow.com/questions/10943088/numpy-max-or-max-which-one-is-faster
                return pdf.min(skipna=skip_na)

            delayed_parts = [_min(part) for part in partitions]

            @delayed
            def _reduce(_df):
                c = pd.concat(_df)
                return c.groupby(c.index).min().to_dict()

            return dd.compute({"min": _reduce(delayed_parts)})[0]

        @staticmethod
        def max(columns, skip_na=True):
            """
            This calculate the min for a columns.
            :param skip_na:
            :param columns:
            :return:
            """

            columns = parse_columns(self, columns)
            df = self[columns]
            partitions = df.to_delayed()

            @delayed
            def _max(pdf):
                # Using numpy min is faster https://stackoverflow.com/questions/10943088/numpy-max-or-max-which-one-is-faster
                return pdf.max(skipna=skip_na)

            delayed_parts = [_max(part) for part in partitions]

            @delayed
            def _reduce(_df):
                c = pd.concat(_df)
                return c.groupby(c.index).max().to_dict()

            return dd.compute({"max":_reduce(delayed_parts)})[0]

        @staticmethod
        def stddev(columns):
            columns = parse_columns(self, columns)
            df = self[columns]

            return {"stddev": {col_name: df[col_name].std() for col_name in columns}}

        @staticmethod
        def mode(columns):
            columns = parse_columns(self, columns)
            df = self[columns]

            f = [df[col_name].astype(str).mode(dropna=True) for col_name in columns]

            @delayed
            def _mode(parts):
                return {"mode": {col_name: p.to_list() for p in parts for col_name in columns}}

            return _mode(f).compute()

    return Cols(self)


DataFrame.cols = property(cols)
