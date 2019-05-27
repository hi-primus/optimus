from pyspark.sql import functions as F

from optimus.helpers.checkit import is_dataframe, is_numeric
from optimus.helpers.functions import parse_columns
from optimus.internals import _z_score_col_name


class ZScore:
    """
    Handle outliers using z Score
    """

    def __init__(self, df, columns, threshold):
        """

        :param df:
        :param columns:
        """
        self.df = df
        self.columns = columns
        self.threshold = threshold

    def _z_score(self, action):
        """
        Get outlier using z score

        :return:
        """
        df = self.df
        columns = self.columns
        threshold = self.threshold

        if not is_dataframe(df):
            raise TypeError("Spark Dataframe expected")

        if not is_numeric(threshold):
            raise TypeError("Numeric expected")

        columns = parse_columns(df, columns)

        for col_name in columns:
            # the column with the z_col value is always the string z_col plus the name of column
            z_col_name = _z_score_col_name(col_name)

            if action is "drop":
                df = df.cols.z_score(col_name,z_col_name) \
                    .rows.drop(F.col(z_col_name) > threshold) \
                    .cols.drop(z_col_name)

            elif action is "select":
                df = df.cols.z_score(col_name) \
                    .rows.select(F.col(z_col_name) > threshold) \
                    .cols.drop(z_col_name)

        return df

    def drop(self):
        return self._z_score("drop")

    def select(self):
        return self._z_score("select")
