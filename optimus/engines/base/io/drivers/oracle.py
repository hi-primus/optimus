from enum import Enum

from optimus.helpers.functions import singleton

from optimus.engines.base.io.drivers.abstract_driver import AbstractDriver
from optimus.engines.base.io.properties import DriverProperties


@singleton
class OracleDriver(AbstractDriver):
    """Oracle Database"""

    def properties(self) -> Enum:
        return DriverProperties.ORACLE

    def url(self, *args, **kwargs) -> str:
        if kwargs["oracle_sid"] is not None:
            return f"""jdbc:oracle+cx_oracle:thin:@{kwargs["host"]}:{kwargs["port"]}/{kwargs["oracle_sid"]}"""
        elif kwargs["oracle_service_name"] is not None:
            return f"""jdbc:oracle+cx_oracle:thin:@//{kwargs["host"]}:{kwargs["port"]}/{kwargs[
                "oracle_service_name"]}"""
        else:
            return f"""jdbc:oracle+cx_oracle:thin:@//{kwargs["oracle_tns"]}"""

    def table_names_query(self, *args, **kwargs) -> str:
        return """
            SELECT table_name, extractvalue(xmltype( dbms_xmlgen.getxml('select count(*) c from '||table_name)), '/ROWSET/ROW/C') count
            FROM user_tables ORDER BY table_name
        """

    def table_name_query(self, *args, **kwargs) -> str:
        return "SELECT table_name as 'table_name' FROM user_tables"

    def count_query(self, *args, **kwargs) -> str:
        return "SELECT COUNT(*) COUNT FROM " + kwargs["db_table"]

    def primary_key_query(self, *args, **kwargs) -> str:
        pass

    def min_max_query(self, *args, **kwargs) -> str:
        return f"""SELECT min({kwargs["partition_column"]}) AS min, max({kwargs["partition_column"]}) AS max FROM {
        kwargs["table_name"]} """
