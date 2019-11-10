from enum import Enum

from singleton_decorator import singleton

from optimus.io.abstract_driver import AbstractDriver
from optimus.io.properties import DriverProperties


@singleton
class SQLServerDriver(AbstractDriver):
    """SQLServer Database"""

    def properties(self) -> Enum:
        return DriverProperties.SQLSERVER

    def url(self, *args, **kwargs) -> str:
        return f"""jdbc:mssql+pymssql://{kwargs["host"]}:{kwargs["port"]};databaseName={kwargs["database"]}"""

    def table_names_query(self, *args, **kwargs):
        return "SELECT * FROM INFORMATION_SCHEMA.TABLES"

    def table_name_query(self, *args, **kwargs) -> str:
        return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"

    def count_query(self, *args, **kwargs) -> str:
        return "SELECT COUNT(*) as COUNT FROM " + kwargs["db_table"]
