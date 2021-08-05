from optimus.engines.base.rows import BaseRows


class Rows(BaseRows):
    """Base class for all Rows implementations"""

    @staticmethod
    def _sort(df, col_name, ascending):
        pass

    @staticmethod
    def unnest(input_cols):
        pass

    def __init__(self, df):
        super(Rows, self).__init__(df)

    def _count(self, compute=True):
        return int(self.root.data.count("*"))
