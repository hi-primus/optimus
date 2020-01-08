from enum import Enum

from singleton_decorator import singleton

from optimus.spark.io.abstract_driver import AbstractDriver
from optimus.spark.io.properties import DriverProperties


@singleton
class MySQLDriver(AbstractDriver):
    """MySQL Database"""

    def properties(self) -> Enum:
        return DriverProperties.MYSQL

    def url(self, *args, **kwargs) -> str:
        # return f"""jdbc:{kwargs["driver"]}://{kwargs["host"]}:{kwargs["port"]}/{kwargs["database"]}?currentSchema={
        # kwargs["schema"]}"""
        print(123)
        return f"""{kwargs["driver"]}://{kwargs["user"]}:{kwargs["password"]}@{kwargs["host"]}:{kwargs["port"]}/{kwargs[
            "database"]}"""

    def table_names_query(self, *args, **kwargs) -> str:
        return "SELECT table_name, table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + \
               kwargs["database"] + "'"

    def table_name_query(self, *args, **kwargs) -> str:
        return "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + \
               kwargs["database"] + "' GROUP BY TABLE_NAME"

    def count_query(self, *args, **kwargs) -> str:
        return "SELECT COUNT(*) as COUNT FROM " + kwargs["db_table"]
