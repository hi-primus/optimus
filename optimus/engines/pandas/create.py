from optimus.engines.base.create import BaseCreate
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.types import *


class Create(BaseCreate):

    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> 'DataFrameType':
        return PandasDataFrame(dfd, *args, **kwargs, op=self.op)
