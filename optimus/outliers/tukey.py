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
        self.upper_bound, self.lower_bound, self.iqr1, self.iqr3 = dict_filter(self.whiskers(),
                                                                               ["upper_bound", "lower_bound", "iqr1",
                                                                                "iqr3"])
        super().__init__(df, col_name)

    def whiskers(self):
        """
        Get the whiskers and IQR
        :return:
        """
        iqr = self.df.cols.iqr(self.col_name, more=True)
        lower_bound = iqr["q1"] - (iqr["iqr"] * 1.5)
        upper_bound = iqr["q3"] + (iqr["iqr"] * 1.5)

        return {"lower_bound": lower_bound, "upper_bound": upper_bound, "iqr1": iqr["q1"], "iqr3": iqr["q3"]}

    def info(self, output: str = "dict"):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """
        lower_bound = self.lower_bound
        upper_bound = self.upper_bound
        iqr1 = self.iqr1
        iqr3 = self.iqr3

        result = {"count_outliers": self.count(), "count_non_outliers": self.non_outliers_count(),
                  "lower_bound": lower_bound, "lower_bound_count": self.count_lower_bound(lower_bound),
                  "upper_bound": upper_bound, "upper_bound_count": self.count_upper_bound(upper_bound),
                  "iqr1": iqr1, "iqr3": iqr3}

        if output == "json":
            result = dump_json(result)
        return result
