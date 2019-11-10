from enum import Enum

from singleton_decorator import singleton

from optimus.io.abstract_driver import AbstractDriver
from optimus.io.properties import DriverProperties


@singleton
class PrestoDriver(AbstractDriver):
    """Presto Database"""

    def properties(self) -> Enum:
        return DriverProperties.PRESTO

    def url(self, *args, **kwargs) -> str:
        return f"""jdbc:{kwargs["driver"]}://{kwargs["host"]}:{kwargs["port"]}/{kwargs["presto_catalog"]}/{kwargs["database"]}"""

    def table_names_query(self, *args, **kwargs) -> str:
        return "SELECT table_name, 0 as table_rows FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '" + \
               kwargs["database"] + "'"

    def table_name_query(self, *args, **kwargs) -> str:
        return "SELECT table_name, 0 as table_rows FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '" + \
               kwargs["database"] + "'"

    def count_query(self, *args, **kwargs) -> str:
        return "SELECT COUNT(*) as COUNT FROM " + kwargs["db_table"]
