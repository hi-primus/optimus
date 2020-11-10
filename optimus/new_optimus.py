from optimus.engines.base.meta import Meta
from optimus.engines.dask.columns import Cols
from optimus.engines.dask.extension import Ext
from optimus.engines.dask.rows import Rows


class DaskDataFrame:
    def __init__(self, df):
        self.data = df
        self.meta_data = {}

    def __getitem__(self, item):
        return self.cols.select(item)

    def __repr__(self):
        self.ext.display()
        return str(type(self))

    def new(self, df):
        return DaskDataFrame(df)

    @property
    def rows(self):
        return Rows(self)

    @property
    def cols(self):
        return Cols(self)

    @property
    def ext(self):
        return Ext(self)

    @property
    def meta(self):
        return Meta(self)
