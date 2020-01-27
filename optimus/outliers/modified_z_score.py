# from pyspark.sql import functions as F

from optimus.helpers.columns import parse_columns, name_col
from optimus.helpers.constants import RELATIVE_ERROR
from optimus.helpers.converter import one_list_to_val
from optimus.infer import is_numeric, is_spark_dataframe
from optimus.outliers.abstract_outliers_threshold import AbstractOutlierThreshold


class ModifiedZScore(AbstractOutlierThreshold):
    """
    Handle outliers from a DataFrame using modified z score
    Reference: http://colingorrie.github.io/outlier-detection.html#modified-z-score-method
    :return:
    """

    def __init__(self, df, col_name, threshold, relative_error=RELATIVE_ERROR):
        """

        :param df:
        :param col_name:
        :param threshold:
        """
        if not is_spark_dataframe(df):
            raise TypeError("Spark Dataframe expected")

        if not is_numeric(threshold):
            raise TypeError("Numeric expected")

        if not is_numeric(relative_error):
            raise TypeError("Numeric expected")

        self.df = df
        self.threshold = threshold
        self.relative_error = relative_error
        self.col_name = one_list_to_val(parse_columns(df, col_name))
        self.df_score = self._m_z_score()
        super().__init__(self.df_score, col_name, "modified_z_score")

    def _m_z_score(self):
        df = self.df
        col_name = self.col_name

        mad = df.cols.mad(col_name, self.relative_error, True)
        m_z_col_name = name_col(col_name, "modified_z_score")

        return df.withColumn(m_z_col_name, F.abs(0.6745 * (df[col_name] - mad["median"]) / mad["mad"]))

    def info(self, output: str = "dict"):
        m_z_col_name = name_col(self.col_name, "modified_z_score")

        df = self._m_z_score()
        max_m_z_score = df.rows.select(df[m_z_col_name] > self.threshold).cols.max(m_z_col_name)

        return {"count_outliers": self.count(), "count_non_outliers": self.non_outliers_count(),
                "max_m_z_score": max_m_z_score}
