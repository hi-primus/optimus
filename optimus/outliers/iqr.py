from pyspark.sql import functions as F

from optimus.helpers.checkit import is_dataframe
from optimus.helpers.functions import parse_columns


class IQR:
    """
        Delete outliers using inter quartile range
    """

    def __init__(self, df, columns):
        """

        :param df:
        :param columns:
        """
        self.df = df
        self.columns = columns

    def _iqr(self, action):
        """
        Select or drop outliers
        :param action:
        :return:
        """
        df = self.df
        columns = self.columns

        if not is_dataframe(self.df):
            raise TypeError("Spark Dataframe expected")

        columns = parse_columns(self.df, columns)

        for col_name in columns:
            iqr = df.cols.iqr(col_name, more=True)
            lower_bound = iqr["q1"] - (iqr["iqr"] * 1.5)
            upper_bound = iqr["q3"] + (iqr["iqr"] * 1.5)

            if action is "drop":
                df = df.rows.drop((F.col(col_name) > upper_bound) | (F.col(col_name) < lower_bound))
            elif action is "select":
                df = df.rows.select((F.col(col_name) > upper_bound) | (F.col(col_name) < lower_bound))

        return df

    def drop(self):
        return self._iqr("drop")

    def select(self):
        return self._iqr("select")
