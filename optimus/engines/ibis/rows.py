import pandas as pd

from optimus.engines.base.rows import BaseRows

DataFrame = pd.DataFrame


class Rows(BaseRows):
    def __init__(self, df):
        super(Rows, self).__init__(df)

    def count(self, compute=True) -> int:
        """
        Count dataframe rows
        """
        dfd = self.root.data
        # TODO: Be sure that we need the compute param
        if compute is True:
            result = dfd.count().execute()
        else:
            result = dfd.count()
        return result

    def _sort(self, dfd, col_name, ascending):
        return dfd.sort_values(col_name, ascending=ascending)
