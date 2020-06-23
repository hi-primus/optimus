import cupy as cp
from dask import dataframe as dd, delayed
from dask_cudf.core import DataFrame as DaskCUDFDataFrame

from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.columns import parse_columns
from optimus.infer import Infer
from optimus.profiler.functions import fill_missing_var_types


def cols(self: DaskCUDFDataFrame):
    class Cols(DaskBaseColumns):
        def __init__(self, df):
            super(DaskBaseColumns, self).__init__(df)


        def count_uniques(self, columns, estimate=True):
            df = self.df
            columns = parse_columns(df, columns)
            count_uniques_values = dd.compute(df[col_name].nunique() for col_name in columns)[0]
            return {column: _uniques for column, _uniques in zip(columns, count_uniques_values)}

        def hist(self, columns, buckets=10, compute=True):
            df = self.df

            @delayed
            def hist_series(_series, _buckets):
                arr = cp.asarray(_series)
                # .to_gpu_array filter nan
                i, j = cp.histogram(cp.array(arr.to_gpu_array()), _buckets)

                i = list(i)
                j = list(j)
                _hist = [{"lower": float(j[index]), "upper": float(j[index + 1]), "count": int(i[index])} for index in
                         range(len(i))]

                return {_series.name: {"hist": _hist}}

            columns = parse_columns(df, columns)
            partitions = df.to_delayed()

            delayed_parts = [hist_series(part[col_name], buckets) for part in partitions for col_name in columns]
            r = dd.compute(*delayed_parts)

            # Flat list of dict
            r = {x: y for i in r for x, y in i.items()}

            return r

        def append(*args, **kwargs):
            return self

        @staticmethod
        def mode(columns):
            # See https://github.com/rapidsai/cudf/issues/3677
            raise NotImplementedError



        def count_by_dtypes(self, columns, infer=False, str_funcs=None, int_funcs=None, mismatch=None):
            df = self.df
            columns = parse_columns(df, columns)
            dtypes = df.cols.dtypes()

            result = {}
            for col_name in columns:
                df_result = df[col_name].map_partitions(Infer.parse_dask, col_name, infer, dtypes, str_funcs,
                                                        int_funcs, meta=str).compute()

                result[col_name] = dict(df_result.value_counts())

            if infer is True:
                for k in result.keys():
                    result[k] = fill_missing_var_types(result[k])
            else:
                result = self.parse_profiler_dtypes(result)

            return result

    return Cols(self)


DaskCUDFDataFrame.cols = property(cols)
