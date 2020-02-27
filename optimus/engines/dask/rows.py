from dask.dataframe.core import DataFrame

from optimus.engines.base.dask.rows import DaskBaseRows


def rows(self):
    class Rows(DaskBaseRows):

        def __init__(self, df):
            super(DaskBaseRows, self).__init__(df)

    return Rows(self)


DataFrame.rows = property(rows)
