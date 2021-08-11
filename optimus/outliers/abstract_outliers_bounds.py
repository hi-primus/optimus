from abc import ABC, abstractmethod

import pprint

from optimus.helpers.columns import parse_columns
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.filters import dict_filter
from optimus.helpers.json import dump_json


class AbstractOutlierBounds(ABC):
    """
     This is a template class to expand the outliers methods
     Also you need to add the new outlier detection method to outliers.py
     """

    def __init__(self, df, col_name: str, lower_bound: int, upper_bound: int):
        """

        :param df: Spark Dataframe
        :param col_name: column name
        """
        self.df = df
        self.col_name = one_list_to_val(parse_columns(df, col_name))
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

    def __repr__(self):
        return pprint.pformat(self.info())

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
        df = self.df
        col_name = self.col_name
        upper_bound, lower_bound = dict_filter(self.whiskers(), ["upper_bound", "lower_bound"])
        return df.rows.select((df[col_name] > upper_bound) | (df[col_name] < lower_bound))

    # TODO: Pass a defined division param instead or run 3 separated jobs
    def hist(self, col_name: str):
        df = self.df
        # lower bound
        lower_bound_hist = df.rows.select(df[col_name] < self.lower_bound).cols.hist(col_name, 20)

        # upper bound
        upper_bound_hist = df.rows.select(df[col_name] > self.upper_bound).cols.hist(col_name, 20)

        # Non outliers
        non_outlier_hist = df.rows.select(
            (df[col_name] >= self.lower_bound) & (df[col_name] <= self.upper_bound)).cols.hist(col_name, 20)

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
        df = self.df
        return df.rows.select(df[col_name] <= self.lower_bound)

    def select_upper_bound(self):
        col_name = self.col_name
        df = self.df
        return df.rows.select(df[col_name] >= self.upper_bound)

    def drop(self):
        """
        Drop outliers rows using the selected column
        :return:
        """

        df = self.df
        col_name = self.col_name
        # upper_bound, lower_bound = dict_filter(self.whiskers(), ["upper_bound", "lower_bound"])
        return df.rows.drop((df[col_name] > self.upper_bound) | (df[col_name] < self.lower_bound))

    def count_lower_bound(self, bound: int):
        """
        Count outlier in the lower bound
        :return:
        """
        col_name = self.col_name
        df = self.df
        return df.rows.select(df[col_name] < bound).rows.count()

    def count_upper_bound(self, bound: int):
        """
        Count outliers in the upper bound
        :return:
        """
        col_name = self.col_name
        df = self.df
        return df.rows.select(df[col_name] > bound).rows.count()

    def count(self):
        """
        Count the outliers rows using the selected column
        :return:
        """
        col_name = self.col_name
        df = self.df
        return df.rows.select((df[col_name] > self.upper_bound) | (df[col_name] < self.lower_bound)).rows.count()

    def non_outliers_count(self):
        """
        Count non outliers rows using the selected column
        :return:
        """
        df = self.df
        col_name = self.col_name
        return df.rows.select((df[col_name] <= self.upper_bound) & (df[col_name] >= self.lower_bound)).rows.count()

    @abstractmethod
    def info(self, output: str = "dict"):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """
        pass
