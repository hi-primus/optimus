from enum import Enum, unique


@unique
class DriverProperties(Enum):
    """Enum holding all driver properties."""

    REDIS = {
        "name": "redis",
        "port": 6379,
        "table_name": ""
    }

    CASSANDRA = {
        "name": "cassandra",
        "port": 9042,
        "table_name": ""
    }
    MYSQL = {
        "name": "mysql",
        "port": 3306,
        "table_name": "table_name"
    }
    ORACLE = {
        "name": "oracle",
        "port": 1521,
        "table_name": "table_name"
    }
    POSTGRESQL = {
        "name": "postgresql",
        "port": 5432,
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
        "table_name": "name"
    }
    PRESTO = {
        "name": "presto",
        "port": 8080,
        "table_name": ""
    }
    REDSHIFT = {
        "name": "redshift",
        "port": 5439,
        "table_name": "table_name"
    }
    BIGQUERY = {
        "name": "bigquery",
        "port": 5439,
        "table_name": "table_name"
    }
    IMPALA = {
        "name": "impala",
        "port": 10000,
        "table_name": "table_name"
    }


    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
