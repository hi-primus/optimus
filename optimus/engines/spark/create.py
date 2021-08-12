from optimus.engines.base.create import BaseCreate
from optimus.helpers.types import *

from optimus.engines.spark.dataframe import SparkDataFrame
from optimus.engines.spark.spark import Spark


class Create(BaseCreate):

    def _df_from_dfd(self, dfd, *args, **kwargs) -> 'DataFrameType':
        dfd = Spark.instance.spark.createDataFrame(dfd)
        return SparkDataFrame(dfd, *args, **kwargs, op=self.op)
