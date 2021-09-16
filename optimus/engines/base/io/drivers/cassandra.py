from enum import Enum

from optimus.engines.base.io.drivers.abstract_driver import AbstractDriver
from optimus.engines.base.io.properties import DriverProperties
from optimus.helpers.functions import singleton


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

    def primary_key_query(self, *args, **kwargs) -> str:
        pass

    def min_max_query(self, *args, **kwargs) -> str:
        return f"""SELECT min({kwargs["partition_column"]}) AS min, max({kwargs["partition_column"]}) AS max FROM {
        kwargs["table_name"]} """
