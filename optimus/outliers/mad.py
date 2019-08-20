from pyspark.sql import functions as F

from optimus.helpers.filters import dict_filter
from optimus.helpers.constants import RELATIVE_ERROR

class MAD:
    """
    Handle outliers using mad
    """

    def __init__(self, df, col_name, threshold, relative_error=RELATIVE_ERROR):
        """

        :param df:
        :param col_name:
        """
        self.df = df
        self.col_name = col_name
        self.threshold = threshold
        self.relative_error = relative_error

    def whiskers(self):
        """
        Get the wisker used to defined outliers
        :return:
        """
        mad_value = self.df.cols.mad(self.col_name, self.relative_error, more=True)
        lower_bound = mad_value["median"] - self.threshold * mad_value["mad"]
        upper_bound = mad_value["median"] + self.threshold * mad_value["mad"]

        return {"lower_bound": lower_bound, "upper_bound": upper_bound}

    def drop(self):
        col_name = self.col_name
        upper_bound, lower_bound = dict_filter(self.whiskers(), ["upper_bound", "lower_bound"])
        return self.df.rows.drop((F.col(col_name) > upper_bound) | (F.col(col_name) < lower_bound))

    def select(self):
        """
        Select outliers rows using the selected column
        :return:
        """
        col_name = self.col_name
        upper_bound, lower_bound = dict_filter(self.whiskers(), ["upper_bound", "lower_bound"])
        return self.df.rows.select((F.col(col_name) > upper_bound) | (F.col(col_name) < lower_bound))

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

        return self.drop().count()

    def info(self):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """
        upper_bound, lower_bound, = dict_filter(self.whiskers(),
                                                ["upper_bound", "lower_bound"])

        return {"count_outliers": self.count(), "count_non_outliers": self.non_outliers_count(),
                "lower_bound": lower_bound,
                "upper_bound": upper_bound, }
