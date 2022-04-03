import polars as pl
from optimus.engines.base.dataframe.rows import DataFrameBaseRows
from optimus.engines.base.pandas.rows import PandasBaseRows
from optimus.engines.base.rows import BaseRows


class Rows(DataFrameBaseRows, PandasBaseRows, BaseRows):
    def _count(self, compute=True) -> int:
        """
        Specific implementation to count the number of rows in the dataframe
        :param compute:
        :return:
        """

        return self.root.data.select([pl.count()]).collect()[0, 0]

    def limit(self, count=10) -> 'DataFrameType':
        """
        Limit the number of rows
        :param count:
        :return:
        """
        # Ensure that count is an integer. Ibis complains otherwise
        count = int(count)
        return self.root.new(self.root.data.collect()[0, count])
