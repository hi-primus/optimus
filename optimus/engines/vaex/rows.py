from optimus.engines.base.rows import BaseRows


class Rows(BaseRows):
    """Base class for all Rows implementations"""

    @staticmethod
    def _sort(df, col_name, ascending):
        pass

    @staticmethod
    def create_id(column="id"):
        pass

    def append(self, dfs, cols_map):
        pass

    @staticmethod
    def between(columns, lower_bound=None, upper_bound=None, invert=False, equal=False, bounds=None):
        pass

    @staticmethod
    def drop_by_dtypes(input_cols, data_type=None):
        pass

    @staticmethod
    def drop_duplicates(input_cols=None):
        pass

    @staticmethod
    def unnest(input_cols):
        pass

    def __init__(self, df):
        super(Rows, self).__init__(df)

    def _count(self, compute=True):
        return int(self.root.data.count("*"))
