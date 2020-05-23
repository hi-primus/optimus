from optimus.helpers.filters import dict_filter
from optimus.helpers.json import dump_json
from optimus.outliers.abstract_outliers_bounds import AbstractOutlierBounds


class Tukey(AbstractOutlierBounds):
    """
    Handle outliers using inter quartile range
    """

    def __init__(self, df, col_name):
        """

        :param df: Spark Dataframe
        :param col_name: column name
        """
        self.df = df
        self.col_name = col_name

        self.lower_bound, self.upper_bound, self.q1, self.median, self.q3, self.iqr = dict_filter(
            self.whiskers(), ["lower_bound", "upper_bound", "q1", "median", "q3", "iqr"]
        )
        # print(self.lower_bound, self.upper_bound)
        super().__init__(df, col_name, self.lower_bound, self.upper_bound)

    def whiskers(self):
        """
        Get the whiskers and IQR
        :return:
        """
        iqr = self.df.cols.iqr(self.col_name, more=True)

        lower_bound = iqr["q1"] - (iqr["iqr"] * 1.5)
        upper_bound = iqr["q3"] + (iqr["iqr"] * 1.5)

        result = {"lower_bound": lower_bound, "upper_bound": upper_bound, "q1": iqr["q1"], "median": iqr["q2"],
                  "q3": iqr["q3"], "iqr": iqr["iqr"]}

        return result

    def info(self, output: str = "dict"):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """

        lower_bound = self.lower_bound
        upper_bound = self.upper_bound

        q1 = self.q1
        median = self.median
        q3 = self.q3
        iqr = self.iqr

        result = {"count_outliers": self.count(), "count_non_outliers": self.non_outliers_count(),
                  "lower_bound": lower_bound, "lower_bound_count": self.count_lower_bound(lower_bound),
                  "upper_bound": upper_bound, "upper_bound_count": self.count_upper_bound(upper_bound),
                  "q1": q1, "median": median, "q3": q3, "iqr": iqr}

        if output == "json":
            result = dump_json(result)
        return result
