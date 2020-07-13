from dask_cudf.core import DataFrame

from optimus.engines.base.dask.rows import DaskBaseRows
from optimus.helpers.columns import parse_columns
from optimus.infer import is_list
import cudf
from dask import dataframe as dd


def rows(self):
    class Rows(DaskBaseRows):

        def __init__(self, df):
            super(DaskBaseRows, self).__init__(df)

        def to_list(self, input_cols):
            """

            :param input_cols:
            :return:
            """
            df = self.df
            input_cols = parse_columns(df, input_cols)
            df = df[input_cols].compute().to_pandas().values.tolist()

            return df

        def append(self, rows):
            """

            :param rows:
            :return:
            """
            df = self.df

            if is_list(rows):
                rows = dd.from_pandas(cudf.DataFrame(rows), npartitions=1)

            # Can not concatenate dataframe with not string columns names
            rows.columns = df.columns

            df = dd.concat([df, rows], axis=0, interleave_partitions=True)

            return df

    return Rows(self)


DataFrame.rows = property(rows)
