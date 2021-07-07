from optimus.helpers.types import DataFrameType
from optimus.engines.base.create import BaseCreate

from optimus.engines.vaex.dataframe import VaexDataFrame


class Create(BaseCreate):

    def _df_from_dfd(self, dfd, *args, **kwargs) -> DataFrameType:
        return VaexDataFrame(dfd, *args, **kwargs)