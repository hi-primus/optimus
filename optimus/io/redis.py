from enum import Enum

from singleton_decorator import singleton

from optimus.io.abstract_driver import AbstractDriver
from optimus.io.properties import DriverProperties


@singleton
class RedisDriver(AbstractDriver):
    """Redis Database"""

    def properties(self) -> Enum:
        return DriverProperties.REDIS

    def url(self, *args, **kwarg) -> str:
        # Redis pyspark do not use and url
        pass

    def table_names_query(self, *args, **kwargs) -> str:
        pass

    def table_name_query(self, *args, **kwargs) -> str:
        pass

    def count_query(self, *args, **kwargs) -> str:
        pass
