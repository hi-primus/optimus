from cudf.core import DataFrame

from optimus.engines.base.dataframe.extension import DataFrameBaseExt


class Ext(DataFrameBaseExt):
    _name = None

    def __init__(self, df):
        super(DataFrameBaseExt, self).__init__(df)

    def to_pandas(self):
        return self.parent.data.to_pandas()