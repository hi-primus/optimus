from optimus.infer import is_numeric
from optimus.helpers.columns import parse_columns, name_col
from optimus.helpers.core import one_list_to_val
from optimus.outliers.abstract_outliers_threshold import AbstractOutlierThreshold


class ZScore(AbstractOutlierThreshold):
    """
    Handle outliers using z Score
    """

    def __init__(self, df, col_name, threshold):
        """

        :para df:
        :param col_name:
        :param threshold:
        """
        if not is_numeric(threshold):
            raise TypeError("Numeric expected")

        self.df = df
        self.threshold = threshold
        self.col_name = one_list_to_val(parse_columns(df, col_name))
        self.tmp_col = name_col(col_name, "z_score")
        self.z_score = self.df[self.col_name].cols.z_score()
        # print("self.df_score",self.df_score)
        # print("self.df",self.df)
        super().__init__()

    def info(self, output="dict"):
        self.tmp_col = name_col(self.col_name, "z_score")

        # df = self.z_score()
        df = self.df
        max_z_score = df.cols.max()

        return {"count_outliers": self.count(), "count_non_outliers": self.non_outliers_count(),
                "max_z_score": max_z_score}
