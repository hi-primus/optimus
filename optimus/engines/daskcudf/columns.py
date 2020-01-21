# import cudf as DataFrame
import dask_cudf as DataFrame

from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.converter import val_to_list


def cols(self: DataFrame):
    class Cols(DaskBaseColumns):
        def __init__(self, df):
            super(DaskBaseColumns, self).__init__(df)

        def append(*args, **kwargs):
            return self

        @staticmethod
        def sort(order="asc", columns=None):
            """
            :param order:
            :param columns:
            :return:
            """
            df = self
            columns = val_to_list(columns)

            df.sort_values(by=columns, ascending=True if order == "asc" else False)
            return df

        @staticmethod
        def mode(columns):
            # See https://github.com/rapidsai/cudf/issues/3677
            raise NotImplementedError


        @staticmethod
        def abs(columns):
            pass

    return Cols(self)
