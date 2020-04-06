from dask_cudf.core import DataFrame as DaskCUDFDataFrame

from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.columns import parse_columns
from optimus.infer import Infer
from optimus.profiler.functions import fill_missing_var_types


def cols(self: DaskCUDFDataFrame):
    class Cols(DaskBaseColumns):
        def __init__(self, df):
            super(DaskBaseColumns, self).__init__(df)

        def append(*args, **kwargs):
            return self

        @staticmethod
        def mode(columns):
            # See https://github.com/rapidsai/cudf/issues/3677
            raise NotImplementedError

        @staticmethod
        def abs(columns):
            pass

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
