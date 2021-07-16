from optimus.engines.base.create import BaseCreate
from optimus.helpers.types import DataFrameType, InternalDataFrameType
import cudf
import pandas as pd

from optimus.engines.dask_cudf.dataframe import DaskCUDFDataFrame

class Create(BaseCreate):

    def _dfd_from_dict(self, dict) -> InternalDataFrameType:
        return cudf.DataFrame({name: cudf.Series(values, dtype=dtype if force_dtype else None) for (name, dtype, nulls, force_dtype), values in dict.items()})

    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> DataFrameType:
        if isinstance(dfd, (pd.DataFrame,)):
            dfd = cudf.from_pandas(dfd)
        return DaskCUDFDataFrame(dfd, n_partitions, *args, **kwargs)
