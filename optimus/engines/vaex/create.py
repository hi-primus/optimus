from optimus.helpers.types import DataFrameType
from optimus.engines.base.create import BaseCreate

from optimus.engines.vaex.dataframe import VaexDataFrame


class Create(BaseCreate):

    def _df_from_dfd(self, dfd, n_partitions=1, *args, **kwargs) -> DataFrameType:
        return VaexDataFrame(dfd, *args, **kwargs)