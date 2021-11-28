from datatable import dt

from optimus.engines.base.dataframe.rows import DataFrameBaseRows
from optimus.engines.base.meta import Meta
from optimus.engines.base.pandas.rows import PandasBaseRows
from optimus.engines.base.rows import BaseRows
from optimus.helpers.constants import Actions
from optimus.infer import is_str, is_list_of_str


class Rows(BaseRows):
    def __init__(self, df):
        super(Rows, self).__init__(df)

    def _count(self, compute=True) -> int:
        df = self.root.data
        return df[:, dt.count()][0, 0]

    def limit(self, count=10) -> 'DataFrameType':
        return self.root.new(self.root.data[:count, :])

    def select(self, expr=None, contains=None, case=None, flags=0, na=False, regex=False) -> 'DataFrameType':
        """
        Return selected rows using an expression
        :param expr: Expression used, For Ex: (df["A"] > 3) & (df["A"] <= 1000) or Column name "A"
        :param contains: List of string
        :param case:
        :param flags:
        :param na:
        :param regex:
        :return:
        """

        df = self.root
        dfd = df.data[expr.get_series(), :]
        meta = Meta.action(df.meta, Actions.SELECT_ROW.value, df.cols.names())

        df = self.root.new(dfd, meta=meta)
        return df
