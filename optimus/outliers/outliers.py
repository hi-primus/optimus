from pyspark.sql import functions as F
from optimus.helpers.functions import parse_columns
from optimus.helpers.checkit import *


class OutlierDetector:
    """
    Outlier detection for pyspark dataframes.
    """

    @staticmethod
    def iqr(df, columns):
        """
        Delete outliers using inter quartile range
        :param df:
        :param columns:
        :return:
        """

        if not is_dataframe(df):
            raise TypeError("Spark Dataframe expected")

        columns = parse_columns(df, columns)

        for c in columns:
            iqr = df.cols.iqr(c, more=True)
            lower_bound = iqr["q1"] - (iqr["iqr"] * 1.5)
            upper_bound = iqr["q3"] + (iqr["iqr"] * 1.5)

            df = df.rows.drop((F.col(c) > upper_bound) | (F.col(c) < lower_bound))

        return df

    @staticmethod
    def z_score(df, columns, threshold=None):
        """
        Delete outlier using z score
        :param df:
        :param columns:
        :param threshold:
        :return:
        """

        if not is_dataframe(df):
            raise TypeError("Spark Dataframe expected")

        if not is_int(threshold):
            raise TypeError("Integer expected")

        columns = parse_columns(df, columns)

        for c in columns:
            # the column with the z_col value is always the string z_col plus the name of column
            z_col = "z_col_" + c

            df = df.cols.z_score(c) \
                .rows.drop(F.col(z_col) > threshold) \
                .cols.drop(z_col)

        return df

    @staticmethod
    def mad(df, columns, threshold=None):
        """
        Delete outlier using mad
        :param df:
        :param columns:
        :param threshold:
        :return:
        """

        if not is_dataframe(df):
            raise TypeError("Spark Dataframe expected")

        if not is_int(threshold):
            raise TypeError("Integer expected")

        columns = parse_columns(df, columns)
        for c in columns:
            mad_value = df.cols.mad(c, more=True)
            lower_bound = mad_value["median"] - threshold * mad_value["mad"]
            upper_bound = mad_value["median"] + threshold * mad_value["mad"]

            df = df.rows.drop((F.col(c) > upper_bound) | (F.col(c) < lower_bound))
        return df
