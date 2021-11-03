from datatable import dt

from optimus.engines.base.dataframe.rows import DataFrameBaseRows
from optimus.engines.base.pandas.rows import PandasBaseRows
from optimus.engines.base.rows import BaseRows


class Rows(DataFrameBaseRows, PandasBaseRows, BaseRows):
    def __init__(self, df):
        super(Rows, self).__init__(df)

    def _count(self, compute=True) -> int:
        df = self.root.data
        return df[:, dt.count()][0, 0]

    def limit(self, count=10) -> 'DataFrameType':
        return self.root.new(self.root.data[:count, :])
