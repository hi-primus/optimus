import cudf
from dask import dataframe as dd

from optimus.engines.base.dask.rows import DaskBaseRows
from optimus.infer import is_list


class Rows(DaskBaseRows):

    def __init__(self, df):
        super(DaskBaseRows, self).__init__(df)

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
