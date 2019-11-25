from optimus.helpers.constants import RELATIVE_ERROR
from optimus.helpers.filters import dict_filter
from optimus.helpers.json import dump_json
from optimus.outliers.abstract_outliers_bounds import AbstractOutlierBounds


class MAD(AbstractOutlierBounds):
    """
    Handle outliers using mad
    """

    def __init__(self, df, col_name, threshold: int, relative_error: int = RELATIVE_ERROR):
        """

        :param df:
        :param col_name:
        :type threshold: object
        :type relative_error: object
        """
        self.df = df
        self.col_name = col_name
        self.threshold = threshold
        self.relative_error = relative_error
        self.upper_bound, self.lower_bound = dict_filter(self.whiskers(), ["upper_bound", "lower_bound"])

        super().__init__(df, col_name)

    def whiskers(self):
        """
        Get the wisker used to defined outliers
        :return:
        """
        mad_value = self.df.cols.mad(self.col_name, self.relative_error, more=True)
        lower_bound = mad_value["median"] - self.threshold * mad_value["mad"]
        upper_bound = mad_value["median"] + self.threshold * mad_value["mad"]

        return {"lower_bound": lower_bound, "upper_bound": upper_bound}

    def info(self, output: str = "dict"):
        """
        Get whiskers, iqrs and outliers and non outliers count
        :return:
        """
        upper_bound, lower_bound, = dict_filter(self.whiskers(),
                                                ["upper_bound", "lower_bound"])

        result = {"count_outliers": self.count(), "count_non_outliers": self.non_outliers_count(),
                  "lower_bound": lower_bound, "lower_bound_count": self.count_lower_bound(lower_bound),
                  "upper_bound": upper_bound, "upper_bound_count": self.count_upper_bound(upper_bound)}
        if output == "json":
            result = dump_json(result)
        return result
