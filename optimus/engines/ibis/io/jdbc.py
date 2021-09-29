import ibis

from optimus.engines.base.io.driver_context import DriverContext
from optimus.engines.base.io.factory import DriverFactory
from optimus.engines.ibis.dataframe import IbisDataFrame
from optimus.engines.pandas.dataframe import PandasDataFrame


class JDBC:
    def __init__(self, host, database, user, password, port=None, driver=None, schema="public", oracle_tns=None,
                 oracle_service_name=None, oracle_sid=None, presto_catalog=None, cassandra_keyspace=None,
                 cassandra_table=None, bigquery_project=None, bigquery_dataset=None, sso=False):
        if database is None:
            database = ""

        self.db_driver = driver
        self.oracle_sid = oracle_sid
        self.cassandra_keyspace = cassandra_keyspace
        self.cassandra_table = cassandra_table

        self.driver_context = DriverContext(DriverFactory.get(self.db_driver))
        self.driver_properties = self.driver_context.properties()

        if port is None:
            self.port = self.driver_properties.value["port"]
        else:
            self.port = port

        self.driver_option = self.driver_properties.value["java_class"]
        self.uri = self.driver_context.uri(
            user=user,
            password=password,
            driver=driver,
            host=host,
            port=str(self.port),
            database=database,
            schema=schema,
            oracle_tns=oracle_tns,
            oracle_sid=oracle_sid,
            oracle_service_name=oracle_service_name,
            presto_catalog=presto_catalog,
            bigquery_project=bigquery_project,
            bigquery_dataset=bigquery_dataset,
            sso=sso
        )
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        print(self.uri)
        # logger.print(self.uri)

    def _connect(self):
        # Override the schema used in the constructor
        # if schema is None:
        #     schema = self.schema

        # hdfs = ibis.impala.hdfs_connect(host='localhost')
        con = getattr(ibis, self.db_driver).connect(user=self.user, password=self.password,
                                                    host=self.host, port=self.port, database=self.database
                                                    )
        return con

    def tables(self, schema=None, database=None, limit=None):

        """
        Return all the tables in a database
        :return:
        """

        return self._connect().list_tables()

    def table_to_df(self, table_name, database=None, columns="*", limit=None):
        """
        Return all the tables in a database
        :return:
        """

        # Override the schema used in the constructor
        if database is None:
            database = self.database

        table = self._connect().table(table_name, database=database)
        # return PandasDataFrame(table.execute())
        return IbisDataFrame(table, op=self.op)

    # def execute(self, query: str, limit: str = None, num_partitions: int = NUM_PARTITIONS, partition_column: str = None,
    #             table_name=None):
    #
    #     """
    #     Execute a SQL query
    #     :param query: SQL query string
    #     :param limit: default limit the whole query. We play defensive here in case the result is a big chunk of data
    #     :param num_partitions:
    #     :param partition_column:
    #     :param table_name: Aimed to be used to with table_to_df() to optimize data big data loading
    #     :return:
    #     """
    #
    #     # hdfs = ibis.impala.hdfs_connect(host='localhost')
    #     con = ibis.impala.connect(user=self.user, password=self.password,
    #                               host=self.host, port=21050, database=self.database
    #                               )
    #
    #     return con
