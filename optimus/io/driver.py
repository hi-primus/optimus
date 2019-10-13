from enum import Enum, unique


@unique
class DriverResolver(Enum):
    """Enum holding all driver properties."""

    CASSANDRA = {"name": "cassandra", "port": 9042}
    MY_SQL = {"name": "mysql", "port": 3306}
    ORACLE = {"name": "oracle", "port": 1521, "class": "oracle.jdbc.OracleDriver"}
    POSTGRES = {"name": "postgres", "port": 5432, "class": "org.postgresql.Driver"}
    POSTGRES_SQL = {"name": "postgresql", "port": 5432, "class": "org.postgresql.Driver"}
    SQL_SERVER = {"name": "sqlserver", "port": 1433, "class": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    SQL_LITE = {"name": "sqlite", "port": 0, "class": "org.sqlite.JDBC"}
    PRESTO = {"name": "presto", "port": 8080, "class": "com.facebook.presto.jdbc.PrestoDriver"}
    REDSHIFT = {"name": "redshift", "port": 5439}

    def __str__(self):
        return self.value["name"]

    def port(self):
        return self.value["port"]

    def java_class(self):
        return self.value["class"]

    def describe(self):
        return self.value["name"], self.value["port"], self.value["class"]

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
