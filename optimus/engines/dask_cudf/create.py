from optimus.engines.base.create import BaseCreate
from optimus.helpers.types import *
import cudf
import pandas as pd

from optimus.engines.base.create import BaseCreate
from optimus.engines.dask_cudf.dataframe import DaskCUDFDataFrame
from optimus.helpers.types import DataFrameType, InternalDataFrameType


class Create(BaseCreate):

    @property
    def _pd(self):
        return cudf

    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> 'DataFrameType':
        if isinstance(dfd, (pd.DataFrame,)):
            dfd = cudf.from_pandas(dfd)
        return DaskCUDFDataFrame(dfd, n_partitions, *args, **kwargs, op=self.op)
