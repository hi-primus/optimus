from enum import Enum

from singleton_decorator import singleton

from optimus.io.abstract_driver import AbstractDriver
from optimus.io.properties import DriverProperties


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
