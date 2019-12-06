from abc import ABC, abstractmethod

from pyspark.sql import functions as F

from optimus.infer import is_dataframe
from optimus.helpers.columns import parse_columns
from optimus.helpers.converter import one_list_to_val, val_to_list
from optimus.helpers.filters import dict_filter
from optimus.helpers.functions import create_buckets
from optimus.helpers.json import dump_json


# LOWER_BOUND =
# UPPER_BOUND =
# OUTLIERS =
# NON_OUTLIERS =

class AbstractOutlierBounds(ABC):
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

    # TODO: Pass a defined division param instead or run 3 separated jobs
    def hist(self, col_name):
        buckets = 20
        min_value, max_value = self.df.cols.range(col_name)

        create_buckets(min_value, self.lower_bound, buckets)

        create_buckets(self.lower_bound, self.upper_bound, buckets)

        create_buckets(self.upper_bound, max_value, buckets)

        # lower bound
        lower_bound_hist = self.df.rows.select(self.df[col_name] < self.lower_bound).cols.hist(col_name, 20)

        # upper bound
        upper_bound_hist = self.df.rows.select(self.df[col_name] > self.upper_bound).cols.hist(col_name, 20)

        # Non outliers
        non_outlier_hist = self.df.rows.select(
            (F.col(col_name) >= self.lower_bound) & (F.col(col_name) <= self.upper_bound)).cols.hist(col_name, 20)


        result = {}
        if lower_bound_hist is not None:
            result.update(lower_bound_hist)
        if upper_bound_hist is not None:
            result.update(upper_bound_hist)
        if non_outlier_hist is not None:
            result.update(non_outlier_hist)

        return dump_json(result)

    def select_lower_bound(self):
        col_name = self.col_name
        sample = {"columns": [{"title": cols} for cols in val_to_list(self.col_name)],
                  "value": self.df.rows.select(self.df[col_name] < self.lower_bound).limit(100).rows.to_list(col_name)}
        return dump_json(sample)

    def select_upper_bound(self):
        col_name = self.col_name
        sample = {"columns": [{"title": cols} for cols in val_to_list(col_name)],
                  "value": self.df.rows.select(self.df[col_name] > self.upper_bound).limit(100).rows.to_list(col_name)}
        return dump_json(sample)

    def drop(self):
        """
        Drop outliers rows using the selected column
        :return:
        """

        col_name = self.col_name
        # upper_bound, lower_bound = dict_filter(self.whiskers(), ["upper_bound", "lower_bound"])
        # print(upper_bound, lower_bound)
        return self.df.rows.drop((F.col(col_name) > self.upper_bound) | (F.col(col_name) < self.lower_bound))

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
        return self.df.rows.select(
            (F.col(col_name) <= self.upper_bound) & (F.col(col_name) >= self.lower_bound)).count()

    @abstractmethod
    def info(self, output: str = "dict"):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """
        pass
