from pyspark.sql import DataFrame

from optimus.outliers.tukey import Tukey
from optimus.outliers.mad import MAD
from optimus.outliers.modified_z_score import ModifiedZScore
from optimus.outliers.z_score import ZScore
from optimus.helpers.constants import RELATIVE_ERROR


class Outliers:
    def __init__(self, df):
        self.df = df

    def tukey(self, columns):
        return Tukey(self.df, columns)

    def z_score(self, columns, threshold):
        return ZScore(self.df, columns, threshold)

    def mad(self, columns, threshold, relative_error=RELATIVE_ERROR):
        return MAD(self.df, columns, threshold, relative_error)

    def modified_z_score(self, columns, threshold, relative_error=RELATIVE_ERROR):
        return ModifiedZScore(self.df, columns, threshold, relative_error)


def outliers(self):
    return Outliers(self)


DataFrame.outliers = property(outliers)
