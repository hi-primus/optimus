from pyspark.sql import functions as F

from optimus.helpers.checkit import is_dataframe, is_float, is_numeric
from optimus.helpers.functions import parse_columns
from optimus.internals import _m_z_score_col_name


class ModifiedZScore:
    """
    Handle outliers from a DataFrame using modified z score
    Reference: http://colingorrie.github.io/outlier-detection.html#modified-z-score-method
    :return:
    """

    def __init__(self, df, columns, threshold):
        """

        :param df:
        :param columns:
        :param threshold:
        """
        self.df = df
        self.columns = columns
        self.threshold = threshold

    def _modified_z_score(self, action):
        """

        :param action:
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
            median = df.cols.median(col_name)
            median_absolute_deviation = df.select(F.abs(F.col(col_name) - median).alias(col_name)).cols.median(col_name)

            m_z_col_name = _m_z_score_col_name(col_name)

            df = df.withColumn(m_z_col_name, F.abs(0.6745 * (F.col(col_name) - median) / median_absolute_deviation))
            if action is "select":
                df = df.rows.select(F.col(m_z_col_name) > threshold)
            elif action is "drop":
                df = df.rows.drop(F.col(m_z_col_name) > threshold)
        return df

    def select(self):
        return self._modified_z_score("select")

    def drop(self):
        return self._modified_z_score("drop")
