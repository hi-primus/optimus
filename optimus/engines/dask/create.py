from optimus.engines.base.create import BaseCreate
from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.helpers.types import *
from optimus.helpers.converter import pandas_to_dask_dataframe


class Create(BaseCreate):
    
    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> 'DataFrameType':
        return DaskDataFrame(pandas_to_dask_dataframe(dfd, n_partitions), *args, **kwargs, op=self.op)
