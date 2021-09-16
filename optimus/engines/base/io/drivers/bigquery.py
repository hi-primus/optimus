from enum import Enum

from optimus.engines.base.io.drivers.abstract_driver import AbstractDriver
from optimus.engines.base.io.properties import DriverProperties
from optimus.helpers.functions import singleton


@singleton
class BigQueryDriver(AbstractDriver):
    """BigQuery Database"""

    def properties(self) -> Enum:
        return DriverProperties.BIGQUERY

    def uri(self, *args, **kwargs) -> str:
        return f"""{kwargs["driver"]}://{kwargs["bigquery_project"]}.{kwargs["bigquery_dataset"]}"""

    def url(self, *args, **kwargs) -> str:
        return f"""bigquery://{kwargs["database"]}?currentSchema={kwargs["schema"]}"""

    def table_names_query(self, *args, **kwargs):
        return "SELECT * FROM INFORMATION_SCHEMA.TABLES"

    def table_name_query(self, *args, **kwargs) -> str:
        return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"

    def count_query(self, *args, **kwargs) -> str:
        return "SELECT COUNT(*) as COUNT FROM " + kwargs["db_table"]

    def primary_key_query(self, *args, **kwargs) -> str:
        pass

    def min_max_query(self, *args, **kwargs) -> str:
        return f"""SELECT min({kwargs["partition_column"]}) AS min, max({kwargs["partition_column"]}) AS max FROM {
        kwargs["table_name"]} """
