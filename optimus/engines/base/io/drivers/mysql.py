from enum import Enum

from optimus.engines.base.io.drivers.abstract_driver import AbstractDriver
from optimus.engines.base.io.properties import DriverProperties
from optimus.helpers.functions import singleton


@singleton
class MySQLDriver(AbstractDriver):
    """MySQL Database"""

    def properties(self) -> Enum:
        return DriverProperties.MYSQL

    def uri(self, *args, **kwargs) -> str:
        return f"""{kwargs["driver"]}://{kwargs["user"]}:{kwargs["password"]}@{kwargs["host"]}:{kwargs["port"]}/{kwargs[
            "database"]}"""

    def url(self, *args, **kwargs) -> str:
        return f"""jdbc:{kwargs["driver"]}://{kwargs["host"]}:{kwargs["port"]}/{kwargs["database"]}?currentSchema={
        kwargs["schema"]}"""

    def table_names_query(self, *args, **kwargs) -> str:
        return "SELECT table_name, table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + \
               kwargs["database"] + "'"

    def table_name_query(self, *args, **kwargs) -> str:
        return "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + \
               kwargs["database"] + "' GROUP BY TABLE_NAME"

    def count_query(self, *args, **kwargs) -> str:
        return "SELECT COUNT(*) as COUNT FROM " + kwargs["db_table"]

    def primary_key_query(self, *args, **kwargs) -> str:
        return f"""SHOW KEYS FROM {kwargs["schema"]}.{kwargs["table_name"]} WHERE Key_name = 'PRIMARY'"""

    def min_max_query(self, *args, **kwargs) -> str:
        return f"""SELECT min({kwargs["partition_column"]}) AS min, max({kwargs["partition_column"]}) AS max FROM {
        kwargs["table_name"]} """
