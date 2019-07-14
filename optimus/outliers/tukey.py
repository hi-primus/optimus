from pyspark.sql import functions as F

from optimus.helpers.check import is_dataframe
from optimus.helpers.converter import one_list_to_val
from optimus.helpers.columns import parse_columns
from optimus.helpers.filters import dict_filter


class Tukey:
    """
    Handle outliers using inter quartile range
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

    def whiskers(self):
        """
        Get the whiskers and  IQR
        :return:
        """
        iqr = self.df.cols.iqr(self.col_name, more=True)
        lower_bound = iqr["q1"] - (iqr["iqr"] * 1.5)
        upper_bound = iqr["q3"] + (iqr["iqr"] * 1.5)

        return {"lower_bound": lower_bound, "upper_bound": upper_bound, "iqr1": iqr["q1"], "iqr3": iqr["q3"]}

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

        return self.df.rows.drop((F.col(col_name) > upper_bound) | (F.col(col_name) < lower_bound))

    def count(self):
        """
        Count the outliers rows using the selected column
        :return:
        """

        return self.df.select().count()

    def non_outliers_count(self):
        """
        Count non outliers rows using the selected column
        :return:
        """

        return self.drop().count()

    def info(self):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """
        upper_bound, lower_bound, iqr1, iqr3 = dict_filter(self.whiskers(),
                                                           ["upper_bound", "lower_bound", "iqr1", "iqr3"])

        return {"count_outliers": self.count(), "count_non_outliers": self.non_outliers_count(),
                "lower_bound": lower_bound,
                "upper_bound": upper_bound, "iqr1": iqr1, "iqr3": iqr3}
