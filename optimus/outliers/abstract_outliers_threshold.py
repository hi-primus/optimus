from abc import ABC, abstractmethod

from pyspark.sql import functions as F

from optimus.infer import is_dataframe
from optimus.helpers.columns import parse_columns, name_col
from optimus.helpers.converter import one_list_to_val


class AbstractOutlierThreshold(ABC):
    """
     This is a template class to expand the outliers methods
     Also you need to add the function to outliers.py
     """

    def __init__(self, df, col_name, prefix):
        """

        :param df: Spark Dataframe
        :param col_name: column name
        """
        if not is_dataframe(df):
            raise TypeError("Spark Dataframe expected")

        self.df = df
        self.col_name = one_list_to_val(parse_columns(df, col_name))
        self.tmp_col = name_col(self.col_name, prefix)

    def select(self):
        """
        Select outliers rows using the selected column
        :return:
        """

        df = self.df
        return df.rows.select(F.col(self.tmp_col) > self.threshold).cols.drop(self.tmp_col)

    def drop(self):
        """
        Drop outliers rows using the selected column
        :return:
        """

        df = self.df
        return df.rows.drop(F.col(self.tmp_col) >= self.threshold).cols.drop(self.tmp_col)

    def count_lower_bound(self, bound):
        """
        Count outlier in the lower bound
        :return:
        """
        col_name = self.col_name
        return self.df.rows.select(self.df[col_name] < bound).count()

    def count_upper_bound(self, bound):
        """
        Count outliers in the upper bound
        :return:
        """
        col_name = self.col_name
        return self.df.rows.select(self.df[col_name] >= bound).count()

    def count(self):
        """
        Count the outliers rows using the selected column
        :return:
        """
        return self.select().count()

    def non_outliers_count(self):
        """
        Count non outliers rows using the selected column
        :return:
        """
        df = self.df
        return df.rows.select(F.col(self.tmp_col) < self.threshold).cols.drop(self.tmp_col).count()

    @abstractmethod
    def info(self, output: str = "dict"):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """
        pass
