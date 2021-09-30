from enum import Enum

from optimus.helpers.functions import singleton

from optimus.engines.base.io.drivers.abstract_driver import AbstractDriver
from optimus.engines.base.io.properties import DriverProperties


@singleton
class PrestoDriver(AbstractDriver):
    """Presto Database"""

    def properties(self) -> Enum:
        return DriverProperties.PRESTO

    def url(self, *args, **kwargs) -> str:
        return f"""jdbc:{kwargs["driver"]}://{kwargs["host"]}:{kwargs["port"]}/{kwargs["presto_catalog"]}/{kwargs[
            "database"]}"""

    def table_names_query(self, *args, **kwargs) -> str:
        return "SELECT table_name, 0 as table_rows FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '" + \
               kwargs["database"] + "'"

    def table_name_query(self, *args, **kwargs) -> str:
        return "SELECT table_name, 0 as table_rows FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '" + \
               kwargs["database"] + "'"

    def count_query(self, *args, **kwargs) -> str:
        return "SELECT COUNT(*) as COUNT FROM " + kwargs["db_table"]

    def primary_key_query(self, *args, **kwargs) -> str:
        pass

    def min_max_query(self, *args, **kwargs) -> str:
        return f"""SELECT min({kwargs["partition_column"]}) AS min, max({kwargs["partition_column"]}) AS max FROM {
        kwargs["table_name"]} """
