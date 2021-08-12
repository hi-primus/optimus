from optimus.helpers.types import *
from optimus.engines.base.create import BaseCreate
import pandas as pd

from optimus.engines.pandas.dataframe import PandasDataFrame


class Create(BaseCreate):

    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> 'DataFrameType':
        return PandasDataFrame(dfd, *args, **kwargs, op=self.op)


