from pyspark.sql import functions as F

from optimus.helpers.checkit import is_dataframe, is_int
from optimus.helpers.functions import parse_columns


class MAD:
    """
    Handle outliers using mad
    """

    def __init__(self, df, columns, threshold):
        """

        :param df:
        :param columns:
        """
        self.df = df
        self.columns = columns
        self.threshold = threshold

    def _mad(self, action):
        """

               :type action:
               :return:
               """

        df = self.df
        columns = self.columns
        threshold = self.threshold

        if not is_dataframe(df):
            raise TypeError("Spark Dataframe expected")

        if not is_int(threshold):
            raise TypeError("Integer expected")

        columns = parse_columns(df, columns)
        for c in columns:
            mad_value = df.cols.mad(c, more=True)
            lower_bound = mad_value["median"] - threshold * mad_value["mad"]
            upper_bound = mad_value["median"] + threshold * mad_value["mad"]

            if action is "select":
                df = df.rows.select((F.col(c) > upper_bound) | (F.col(c) < lower_bound))
            elif action is "drop":
                df = df.rows.drop((F.col(c) > upper_bound) | (F.col(c) < lower_bound))
        return df

    def drop(self):
        return self._mad("drop")

    def select(self):
        return self._mad("select")
