from optimus.helpers.types import *
from optimus.engines.base.create import BaseCreate
import pandas as pd

from optimus.engines.polars.dataframe import PolarsDataFrame


class Create(BaseCreate):

    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> 'DataFrameType':
        return PolarsDataFrame(dfd, *args, **kwargs, op=self.op)


