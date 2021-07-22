from optimus.helpers.converter import pandas_to_vaex_dataframe
from optimus.helpers.types import DataFrameType, InternalDataFrameType
from optimus.engines.base.create import BaseCreate

from optimus.engines.vaex.dataframe import VaexDataFrame
import vaex

try:
    import pandas as pd
    InternalPandasDataFrame = pd.DataFrame
except:
    InternalPandasDataFrame = type(None)

class Create(BaseCreate):

    def _dfd_from_dict(self, dict) -> InternalDataFrameType:
        dfd = vaex.from_dict({name: values for (name, dtype, nulls, force_dtype), values in dict.items()})
        for (name, dtype, nulls, force_dtype) in dict.keys():
            if force_dtype:
                dfd[name] = dfd[name].astype(dtype)
        return dfd


    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> DataFrameType:
        if isinstance(dfd, (InternalPandasDataFrame,)):
            dfd = pandas_to_vaex_dataframe(dfd)
        return VaexDataFrame(dfd, *args, **kwargs)
