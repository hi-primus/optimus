import pandas as pd

from optimus.engines.base.dataframe.extension import DataFrameBaseExt

DataFrame = pd.DataFrame


def ext(self: DataFrame):
    class Ext(DataFrameBaseExt):

        def __init__(self, df):
            super().__init__(df)

    return Ext(self)


DataFrame.ext = property(ext)
