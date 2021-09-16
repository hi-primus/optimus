from enum import Enum, unique


@unique
class DriverProperties(Enum):
    """Enum holding all driver properties."""

    REDIS = {
        "name": "redis",
        "port": 6379,
        "table_name": "",
        "java_class":""
    }

    CASSANDRA = {
        "name": "cassandra",
        "port": 9042,
        "table_name": "",
        "java_class": ""
    }
    MYSQL = {
        "name": "mysql",
        "port": 3306,
        "table_name": "table_name",
        "java_class": ""
    }
    ORACLE = {
        "name": "oracle",
        "port": 1521,
        "table_name": "table_name",
        "java_class": ""
    }
    POSTGRESQL = {
        "name": "postgresql",
        "port": 5432,
        "table_name": "table_name",
        "java_class": ""
    }
    SQLSERVER = {
        "name": "mssql+pyodbc",
        "port": 1433,
        "table_name": "TABLE_NAME",
        "java_class": ""
    }
    SQLITE = {
        "name": "sqlite",
        "port": 0,
        "table_name": "name",
        "java_class": ""
    }
    PRESTO = {
        "name": "presto",
        "port": 8080,
        "table_name": "",
        "java_class": ""
    }
    REDSHIFT = {
        "name": "redshift",
        "port": 5439,
        "table_name": "table_name",
        "java_class": ""
    }
    BIGQUERY = {
        "name": "bigquery",
        "port": 5439,
        "table_name": "table_name",
        "java_class": ""
    }
    IMPALA = {
        "name": "impala",
        "port": 10000,
        "table_name": "table_name",
        "java_class": ""
    }


    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
