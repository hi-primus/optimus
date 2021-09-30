from enum import Enum

from optimus.engines.base.io.drivers.abstract_driver import AbstractDriver
from optimus.engines.base.io.properties import DriverProperties
from optimus.helpers.functions import singleton


@singleton
class SQLiteDriver(AbstractDriver):
    """SQLite Database"""

    def properties(self) -> Enum:
        return DriverProperties.SQLITE

    def url(self, *args, **kwargs) -> str:
        return f"""jdbc:{kwargs["driver"]}:{kwargs["host"]}"""

    def table_names_query(self, *args, **kwargs) -> str:
        return "SELECT name FROM sqlite_master WHERE type='table'"

    def table_name_query(self, *args, **kwargs) -> str:
        return "SELECT name FROM sqlite_master WHERE type='table'"

    def count_query(self, *args, **kwargs) -> str:
        return "SELECT COUNT(*) as COUNT FROM " + kwargs["db_table"]

    def primary_key_query(self, *args, **kwargs) -> str:
        pass

    def min_max_query(self, *args, **kwargs) -> str:
        return f"""SELECT min({kwargs["partition_column"]}) AS min, max({kwargs["partition_column"]}) AS max FROM {
        kwargs["table_name"]} """
