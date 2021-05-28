from abc import abstractmethod

import pandas as pd

from optimus.engines.base.dataframe.columns import DataFrameBaseColumns

class PandasBaseColumns(DataFrameBaseColumns):

    def _names(self):
        return list(self.root.data.columns)

    @abstractmethod
    def _pd(self):
        pass

    def append(self, dfs):
        """

        :param dfs:
        :return:
        """

        df = self.root
        dfd = self._pd.concat([df.data.reset_index(drop=True), *[_df.data.reset_index(drop=True) for _df in dfs]], axis=1)
        return self.root.new(dfd)