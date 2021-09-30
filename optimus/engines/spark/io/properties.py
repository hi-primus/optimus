from enum import Enum, unique


@unique
class DriverProperties(Enum):
    """Enum holding all driver properties."""

    REDIS = {
        "name": "redis",
        "port": 6379,
        "java_class": "org.apache.spark.sql.redis",
        "table_name": ""
    }

    CASSANDRA = {
        "name": "cassandra",
        "port": 9042,
        "java_class": "org.apache.spark.sql.cassandra",
        "table_name": ""
    }
    MYSQL = {
        "name": "mysql",
        "port": 3306,
        "java_class": "",
        "table_name": "table_name"
    }
    ORACLE = {
        "name": "oracle",
        "port": 1521,
        "java_class": "oracle.jdbc.OracleDriver",
        "table_name": "table_name"
    }
    POSTGRESQL = {
        "name": "postgresql",
        "port": 5432,
        "java_class": "org.postgresql.Driver",
        "table_name": "table_name"
    }
    SQLSERVER = {
        "name": "mssql+pyodbc",
        "port": 1433,
        "table_name": "TABLE_NAME"
    }
    SQLITE = {
        "name": "sqlite",
        "port": 0,
        "java_class": "org.sqlite.JDBC",
        "table_name": "name"
    }
    PRESTO = {
        "name": "presto",
        "port": 8080,
        "java_class": "com.facebook.presto.jdbc.PrestoDriver",
        "table_name": ""
    }
    REDSHIFT = {
        "name": "redshift",
        "port": 5439,
        "java_class": "",
        "table_name": "table_name"
    }
    BIGQUERY = {
        "name": "bigquery",
        "port": 5439,
        "java_class": "",
        "table_name": "table_name"
    }
    IMPALA = {
        "name": "impala",
        "port": 10000,
        "java_class": "",
        "table_name": "table_name"
    }


    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
