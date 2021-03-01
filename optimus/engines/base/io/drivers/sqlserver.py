from enum import Enum

from optimus.engines.base.io.drivers.abstract_driver import AbstractDriver
from optimus.engines.spark.io.properties import DriverProperties
from optimus.helpers.functions import singleton


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

    def primary_key_query(self, *args, **kwargs) -> str:
        pass

    def min_max_query(self, *args, **kwargs) -> str:
        return f"""SELECT min({kwargs["partition_column"]}) AS min, max({kwargs["partition_column"]}) AS max FROM {
        kwargs["table_name"]} """
