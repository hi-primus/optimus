import databricks.koalas as ks
import pandas as pd

from optimus.engines.base.create import BaseCreate
from optimus.helpers.types import *

from optimus.engines.spark.dataframe import SparkDataFrame


class Create(BaseCreate):

    @property
    def _pd(self):
        return ks

    def _dfd_from_dict(self, dict) -> 'InternalDataFrameType':
        # TODO use Spark instead of Koalas to allow nullable types and create dataframes without using Pandas
        pd_dict = {}
        for (name, dtype, nulls, force_dtype), values in dict.items():
            # use str as default type to avoid conversion error
            dtype = self.op.constants.COMPATIBLE_DTYPES.get(dtype, dtype) if force_dtype else str
            pd_series = pd.Series(values, dtype=dtype)
            pd_dict.update({name: pd_series})
        return self._pd.DataFrame(pd_dict)

    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> 'DataFrameType':
        return SparkDataFrame(dfd, *args, **kwargs, op=self.op)
