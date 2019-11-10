from optimus.helpers.raiseit import RaiseIt
from optimus.io.abstract_driver import AbstractDriver
from optimus.io.cassandra import CassandraDriver
from optimus.io.mysql import MySQLDriver
from optimus.io.oracle import OracleDriver
from optimus.io.postgresql import PostgreSQLDriver
from optimus.io.presto import PrestoDriver
from optimus.io.properties import DriverProperties
from optimus.io.redshift import RedshiftDriver
from optimus.io.sqlite import SQLiteDriver
from optimus.io.sqlserver import SQLServerDriver


class DriverFactory:
    """Database driver factory. This database driver factory currently supports the following implementations:

        * Cassandra
        * MySQL
        * Oracle
        * Postgres
        * Presto
        * Redshift
        * SQLite
        * SQLServer
    """

    @staticmethod
    def get(driver_type) -> AbstractDriver:
        """
        Returns a driver implementation given a database name

        :param driver_type: name of the database
        :return: a database driver
        """
        if driver_type == DriverProperties.CASSANDRA.value["name"]:
            return CassandraDriver()
        elif driver_type == DriverProperties.MYSQL.value["name"]:
            return MySQLDriver()
        elif driver_type == DriverProperties.ORACLE.value["name"]:
            return OracleDriver()
        elif driver_type == DriverProperties.POSTGRESQL.value["name"]:
            return PostgreSQLDriver()
        elif driver_type == DriverProperties.PRESTO.value["name"]:
            return PrestoDriver()
        elif driver_type == DriverProperties.REDSHIFT.value["name"]:
            return RedshiftDriver()
        elif driver_type == DriverProperties.SQLITE.value["name"]:
            return SQLiteDriver()
        elif driver_type == DriverProperties.SQLSERVER.value["name"]:
            return SQLServerDriver()
        else:
            RaiseIt.value_error(driver_type, [database["name"] for database in DriverProperties.list()])
