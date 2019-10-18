from dask.dataframe.core import DataFrame


def rows(self):
    class Rows:
        @staticmethod
        def count() -> int:
            """
            Count dataframe rows
            """
            return len(self)

    return Rows()


DataFrame.rows = property(rows)
