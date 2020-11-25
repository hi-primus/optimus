import pandas as pd

from optimus.engines.base.dataframe.extension import Ext
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import BUFFER_SIZE

DataFrame = pd.DataFrame
Series = pd.Series
import time

# def ext(self: DataFrame):
class Ext(Ext):

    def __init__(self, df):
        super().__init__(df)

    def to_pandas(self):
        return self.parent.data


