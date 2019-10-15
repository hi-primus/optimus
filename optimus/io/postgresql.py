from enum import Enum

from singleton_decorator import singleton

from optimus.io.abstract_driver import AbstractDriver
from optimus.io.properties import DriverProperties


@singleton
class PostgreSQLDriver(AbstractDriver):
    """PostgreSQL Database"""

    def properties(self) -> Enum:
        return DriverProperties.POSTGRESQL

    def url(self, *args, **kwargs) -> str:
        return f"""jdbc:postgresql://{kwargs["host"]}:{kwargs["port"]}/{kwargs["database"]}?currentSchema={kwargs["schema"]}"""

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

    def count_query(self, *args, **kwarg) -> str:
        return "SELECT COUNT(*) as COUNT FROM " + kwarg["db_table"]
