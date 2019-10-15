from enum import Enum

from singleton_decorator import singleton

from optimus.io.abstract_driver import AbstractDriver
from optimus.io.properties import DriverProperties


@singleton
class CassandraDriver(AbstractDriver):
    """Cassandra Database"""

    def properties(self) -> Enum:
        return DriverProperties.CASSANDRA

    def url(self, *args, **kwargs) -> str:
        return ""

    def table_names_query(self, *args, **kwargs) -> str:
        pass

    def table_name_query(self, *args, **kwargs) -> str:
        pass

    def count_query(self, *args, **kwargs) -> str:
        pass
