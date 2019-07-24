from pyspark.sql import functions as F

from optimus.helpers.check import is_dataframe, is_numeric
from optimus.helpers.converter import one_list_to_val
from optimus.helpers.columns import parse_columns, name_col


class ZScore:
    """
    Handle outliers using z Score
    """

    def __init__(self, df, col_name, threshold):
        """

        :param df: Spark Dataframe
        :param col_name:
        """

        if not is_dataframe(df):
            raise TypeError("Spark Dataframe expected")

        self.df = df

        if not is_numeric(threshold):
            raise TypeError("Numeric expected")
        self.threshold = threshold

        self.col_name = one_list_to_val(parse_columns(df, col_name))

    def drop(self):
        col_name = self.col_name
        z_col_name = name_col(col_name, "z_score")
        threshold = self.threshold

        return self.df.cols.z_score(col_name, z_col_name) \
            .rows.drop(F.col(z_col_name) > threshold) \
            .cols.drop(z_col_name)

    def select(self):
        col_name = self.col_name
        z_col_name = name_col(col_name, "z_score")

        return self.df.cols.z_score(col_name, z_col_name) \
            .rows.select(F.col(z_col_name) > self.threshold) \
            .cols.drop(z_col_name)

    def non_outliers_count(self):
        return self.drop().count()

    def count(self):
        return self.select().count()

    def info(self):
        col_name = self.col_name
        z_col_name = name_col(col_name, "z_score")

        max_z_score = self.df.cols.z_score(col_name, z_col_name) \
            .cols.max(z_col_name)

        return {"count_outliers": self.count(), "count_non_outliers": self.non_outliers_count(), "max_z_score": max_z_score}
