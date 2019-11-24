from abc import ABC, abstractmethod

from pyspark.sql import functions as F

from optimus.helpers.check import is_dataframe
from optimus.helpers.columns import parse_columns
from optimus.helpers.converter import one_list_to_val
from optimus.helpers.filters import dict_filter


class AbstractOutlier(ABC):
    """
     This is a template class to expand the outliers methods
     Also you need to add the function to outliers.py
     """

    def __init__(self, df, col_name):
        """

        :param df: Spark Dataframe
        :param col_name: column name
        """
        if not is_dataframe(df):
            raise TypeError("Spark Dataframe expected")

        self.df = df
        self.col_name = one_list_to_val(parse_columns(df, col_name))

    @abstractmethod
    def whiskers(self):
        """
        Get the whiskers and IQR
        :return:
        """
        pass

    def select(self):
        """
        Select outliers rows using the selected column
        :return:
        """

        col_name = self.col_name
        upper_bound, lower_bound = dict_filter(self.whiskers(), ["upper_bound", "lower_bound"])

        return self.df.rows.select((F.col(col_name) > upper_bound) | (F.col(col_name) < lower_bound))

    def drop(self):
        """
        Drop outliers rows using the selected column
        :return:
        """

        col_name = self.col_name
        upper_bound, lower_bound = dict_filter(self.whiskers(), ["upper_bound", "lower_bound"])
        print(upper_bound, lower_bound)
        return self.df.rows.drop((F.col(col_name) > upper_bound) | (F.col(col_name) < lower_bound))

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
        return self.df.rows.select(self.df[col_name] > bound).count()

    def count(self):
        """
        Count the outliers rows using the selected column
        :return:
        """
        col_name = self.col_name
        return self.df.rows.select((F.col(col_name) > self.upper_bound) | (F.col(col_name) < self.lower_bound)).count()

    def non_outliers_count(self):
        """
        Count non outliers rows using the selected column
        :return:
        """
        col_name = self.col_name
        return self.df.rows.select((F.col(col_name) <= self.upper_bound) | (F.col(col_name) >= self.lower_bound)).count()

    @abstractmethod
    def info(self):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """
        pass
