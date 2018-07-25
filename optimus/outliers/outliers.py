from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F

from optimus.helpers.functions import parse_columns


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

        columns = parse_columns(df, columns)

        for c in columns:
            iqr = df.cols().iqr(c, more=True)
            lower_bound = iqr["q1"] - (iqr["iqr"] * 1.5)
            upper_bound = iqr["q3"] + (iqr["iqr"] * 1.5)
            return df.where((F.col(c) > upper_bound) | (F.col(c) < lower_bound))

    @staticmethod
    def z_score(df, columns, threshold=1):
        """
        Delete outlier using z score
        :param df:
        :param columns:
        :param threshold:
        :return:
        """

        columns = parse_columns(df, columns)
        for c in columns:
            # the column with the z_col value is always the string z_col plus the name of column
            z_col = "z_col_" + c

            df = df.cols().z_score(c) \
                .rows().drop(F.col(z_col) > threshold) \
                .cols().drop(z_col)

        return df

    @staticmethod
    def mad():
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        self._df = df
        self._column = column

        self.median_value = median(self._df, self._column)

        absolute_deviation = (self._df
                              .select(self._column)
                              .orderBy(self._column)
                              .withColumn(self._column, absspark(col(self._column) - self.median_value))
                              .cache())

        self.mad_value = median(absolute_deviation, column)

        self.threshold = threshold

        self._limits = []
        self._limits.append(round((self.median_value - self.threshold * self.mad_value), 2))
        self._limits.append(round((self.median_value + self.threshold * self.mad_value), 2))
