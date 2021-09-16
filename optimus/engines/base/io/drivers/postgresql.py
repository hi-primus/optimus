from enum import Enum

from optimus.helpers.functions import singleton

from optimus.engines.base.io.drivers.abstract_driver import AbstractDriver
from optimus.engines.base.io.properties import DriverProperties


@singleton
class PostgreSQLDriver(AbstractDriver):
    """PostgreSQL Database"""

    def properties(self) -> Enum:
        return DriverProperties.POSTGRESQL

    def uri(self, *args, **kwargs) -> str:
        return f"""{kwargs["driver"]}://{kwargs["user"]}:{kwargs["password"]}@{kwargs["host"]}:{kwargs["port"]}/{kwargs[
            "database"]}"""

    def url(self, *args, **kwargs) -> str:
        return f"""jdbc:postgresql://{kwargs["host"]}:{kwargs["port"]}/{kwargs["database"]}?currentSchema={kwargs[
            "schema"]}"""

    def table_names_query(self, *args, **kwargs) -> str:
        return """
            SELECT relname as table_name,cast (reltuples as integer) AS count 
            FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) 
            WHERE nspname IN ('""" + kwargs["schema"] + """') AND relkind='r' ORDER BY reltuples DESC
        """

    def table_name_query(self, *args, **kwargs) -> str:
        return """
            SELECT relname as table_name 
            FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) 
            WHERE nspname IN ('""" + kwargs["schema"] + """') AND relkind='r' ORDER BY reltuples DESC
        """

    def count_query(self, *args, **kwargs) -> str:
        return "SELECT COUNT(*) as COUNT FROM " + kwargs["db_table"]

    def primary_key_query(self, *args, **kwargs) -> str:
        return f"""SHOW KEYS FROM {kwargs["schema"]}.{kwargs["table_name"]} WHERE Key_name = 'PRIMARY'"""

    def min_max_query(self, *args, **kwargs) -> str:
        return f"""SELECT min({kwargs["partition_column"]}) AS min, max({kwargs["partition_column"]}) AS max FROM {
        kwargs["table_name"]} """
