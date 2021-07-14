from optimus.engines.base.create import BaseCreate
from optimus.helpers.types import DataFrameType, InternalDataFrameType
import cudf
import pandas as pd

from optimus.engines.cudf.dataframe import CUDFDataFrame


class Create(BaseCreate):

    def _dfd_from_dict(self, dict) -> InternalDataFrameType:
        return cudf.DataFrame({ name: cudf.Series(values, dtype=dtype) for (name, dtype, nulls), values in dict.items() })

    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> DataFrameType:
        if isinstance(dfd, (pd.DataFrame,)):
            dfd = cudf.from_pandas(dfd)
        return CUDFDataFrame(dfd, *args, **kwargs)
