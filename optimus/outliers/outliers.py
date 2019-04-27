from pyspark.sql import DataFrame

from optimus.outliers.iqr import IQR
from optimus.outliers.mad import MAD
from optimus.outliers.modified_z_score import ModifiedZScore
from optimus.outliers.z_score import ZScore


class Outliers:
    def __init__(self, df):
        self.df = df

    def iqr(self, columns):
        return IQR(self.df, columns)

    def z_score(self, columns, threshold):
        return ZScore(self.df, columns, threshold)

    def mad(self, columns, threshold):
        return MAD(self.df, columns, threshold)

    def modified_z_score(self, columns, threshold):
        return ModifiedZScore(self.df, columns, threshold)


def outliers(self):
    return Outliers(self)


DataFrame.outliers = property(outliers)
