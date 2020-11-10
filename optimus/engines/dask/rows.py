from optimus.engines.base.dask.rows import DaskBaseRows


class Rows(DaskBaseRows):

    def __init__(self, df):
        super(DaskBaseRows, self).__init__(df)
