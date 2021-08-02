
from optimus.helpers.constants import RELATIVE_ERROR
from optimus.outliers.mad import MAD
from optimus.outliers.modified_z_score import ModifiedZScore
from optimus.outliers.tukey import Tukey
from optimus.outliers.z_score import ZScore


class Outliers:
    def __init__(self, df):
        self.df = df

    def tukey(self, cols):
        return Tukey(self.df, cols)

    def z_score(self, cols, threshold):
        return ZScore(self.df, cols, threshold)

    def mad(self, cols, threshold, relative_error=RELATIVE_ERROR):
        return MAD(self.df, cols, threshold, relative_error)

    def modified_z_score(self, cols, threshold, relative_error=RELATIVE_ERROR):
        return ModifiedZScore(self.df, cols, threshold, relative_error)
