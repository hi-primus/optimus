from optimus.engines.base.io.drivers.impala import ImpalaDriver
from optimus.helpers.raiseit import RaiseIt
from optimus.engines.base.io.drivers.abstract_driver import AbstractDriver
from optimus.engines.base.io.drivers.cassandra import CassandraDriver
from optimus.engines.base.io.drivers.mysql import MySQLDriver
from optimus.engines.base.io.drivers.oracle import OracleDriver
from optimus.engines.base.io.drivers.postgresql import PostgreSQLDriver
from optimus.engines.base.io.drivers.presto import PrestoDriver
from optimus.engines.base.io.properties import DriverProperties
from optimus.engines.base.io.drivers.redshift import RedshiftDriver
from optimus.engines.base.io.drivers.sqlite import SQLiteDriver
from optimus.engines.base.io.drivers.sqlserver import SQLServerDriver
from optimus.engines.base.io.drivers.bigquery import BigQueryDriver


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
        * BigQuery
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
        elif driver_type == DriverProperties.BIGQUERY.value["name"]:
            return BigQueryDriver()
        elif driver_type == DriverProperties.IMPALA.value["name"]:
            return ImpalaDriver()
        else:
            RaiseIt.value_error(driver_type, [database["name"] for database in DriverProperties.list()])
