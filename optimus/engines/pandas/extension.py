import pandas as pd

from optimus.engines.base.dataframe.extension import DataFrameBaseExt

DataFrame = pd.DataFrame
Series = pd.Series


# def ext(self: DataFrame):
class Ext(DataFrameBaseExt):

    def __init__(self, df):
        super().__init__(df)
