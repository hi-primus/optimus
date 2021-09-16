from optimus.engines.base.constants import LIMIT_TABLE, NUM_PARTITIONS
from optimus.engines.base.io.driver_context import DriverContext
from optimus.engines.base.io.factory import DriverFactory
from optimus.engines.base.io.properties import DriverProperties
from optimus.engines.spark.spark import Spark
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import collect_as_list
from optimus.helpers.logger import logger
from optimus.engines.spark.dataframe import SparkDataFrame

# Optimus play defensive with the number of rows to be retrieved from the server so if a limit is not specified it will
# only will retrieve the LIMIT value


class JDBC:
    """
    Helper for JDBC connections and queries
    """

    def __init__(self, host, database, user, password, port=None, driver=None, schema="public", oracle_tns=None,
                 oracle_service_name=None, oracle_sid=None, presto_catalog=None, cassandra_keyspace=None,
                 cassandra_table=None):

        """
        Create the JDBC connection object
        :return:
        """

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

        self.driver_option = self.driver_properties.value["java_class"]
        self.url = self.driver_context.url(
            driver=driver,
            host=host,
            port=str(self.port),
            database=database,
            schema=schema,
            oracle_tns=oracle_tns,
            oracle_sid=oracle_sid,
            oracle_service_name=oracle_service_name,
            presto_catalog=presto_catalog
        )
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        logger.print(self.url)

    def tables(self, schema=None, database=None, limit=None):
        """
        Return all the tables in a database
        :return:
        """

        # Override the schema used in the constructor
        if database is None:
            database = self.database

        if schema is None:
            schema = self.schema
        query = self.driver_context.table_names_query(schema=schema, database=database)
        df = self.execute(query, limit)
        return df.display(limit)

    @property
    def table(self):
        """
        Print n rows of every table in a database
        :return: Table Object
        """
        return Table(self)

    def table_to_df(self, table_name, schema="public", columns="*", limit=None, partition_column=None,
                    num_partition=NUM_PARTITIONS):
        """
        Return cols as Spark data frames from a specific table
        :param partition_column:
        :param table_name:
        :param schema:
        :param columns:
        :param limit: how many rows will be retrieved
        """

        if schema is None:
            schema = self.schema

        # db_table = table_name

        query = self.driver_context.count_query(db_table=table_name)
        if limit == "all":
            count = self.execute(query, "all").first()[0]

            # We want to count the number of rows to warn the users how much it can take to bring the whole data
            print(str(int(count)) + " rows")

        if columns == "*":
            columns_sql = "*"
        else:
            columns = val_to_list(columns)
            columns_sql = ",".join(columns)

        # min, max = self.execute(query, limit)

        query = "SELECT " + columns_sql + " FROM " + table_name

        logger.print(query)
        df = self.execute(query, limit, partition_column=partition_column, table_name=table_name,
                          num_partitions=num_partition)

        # Bring the data to local machine if not every time we call an action is going to be
        # retrieved from the remote server
        df = df.run()
        return SparkDataFrame(df, op=self.op)

    def _build_conf(self, query=None, limit=None):
        """
        Build a conf Spark object to make a single SQL query
        :param query:
        :return:
        """
        if limit is None:
            limit = "LIMIT " + str(LIMIT_TABLE)
        elif limit == "all":
            limit = ""
        else:
            limit = "LIMIT " + (str(limit))

        conf = Spark.instance.spark.read \
            .format(
            "jdbc" if not self.db_driver == DriverProperties.CASSANDRA.value["name"] else
            DriverProperties.CASSANDRA.value["java_class"]) \
            .option("url", self.url) \
            .option("user", self.user)

        if query:
            # play defensive with a select clause
            if self.db_driver == DriverProperties.ORACLE.value["name"]:
                query = "(" + query + ") t"
            elif self.db_driver == DriverProperties.PRESTO.value["name"]:
                query = "(" + query + ")"
            elif self.db_driver == DriverProperties.CASSANDRA.value["name"]:
                query = query
            else:
                query = f"""({query} {limit} ) AS t"""

            conf.option("dbtable", query)

        if self.db_driver != DriverProperties.PRESTO.value["name"] and self.password is not None:
            conf.option("password", self.password)

        # Driver
        if self.db_driver == DriverProperties.ORACLE.value["name"] \
                or self.db_driver == DriverProperties.POSTGRESQL.value["name"] \
                or self.db_driver == DriverProperties.PRESTO.value["name"]:
            conf.option("driver", self.driver_option)

        if self.db_driver == DriverProperties.CASSANDRA.value["name"]:
            conf.options(table=self.cassandra_table, keyspace=self.cassandra_keyspace)

        logger.print(query)
        logger.print(self.url)
        return conf

    def execute(self, query: str, limit: str = None, num_partitions: int = NUM_PARTITIONS, partition_column: str = None,
                table_name=None):

        """
        Execute a SQL query
        :param query: SQL query string
        :param limit: default limit the whole query. We play defensive here in case the result is a big chunk of data
        :param num_partitions:
        :param partition_column:
        :param table_name: Aimed to be used to with table_to_df() to optimize data big data loading
        :return:
        """

        conf = self._build_conf(query, limit)

        if partition_column:
            min_max_query = self.driver_context.min_max_query(partition_column=partition_column, table_name=table_name)
            min_value, max_value = collect_as_list(self._build_conf(min_max_query).load())

            conf \
                .option("partitionColumn", partition_column) \
                .option("lowerBound", min_value) \
                .option("upperBound", max_value)

        if num_partitions:
            conf \
                .option("numPartitions", num_partitions)

        return conf.load()

    def df_to_table(self, df, table, mode="overwrite"):
        """
        Send a spark to the database
        :param df:
        :param table:
        :param mode
        :return:
        """
        # Parse array and vector to string. JDBC can not handle this data types
        columns = df.cols.names("*", by_dtypes=["array", "vector"])
        df = df.cols.cast(columns, "str")

        conf = df.data.write \
            .format(
            "jdbc" if not self.db_driver == DriverProperties.CASSANDRA.value["name"] else
            DriverProperties.CASSANDRA.value["java_class"]) \
            .mode(mode) \
            .option("url", self.url) \
            .option("dbtable", table) \
            .option("user", self.user) \
            .option("password", self.password)

        if self.db_driver == DriverProperties.ORACLE.value["name"]:
            conf.option("driver", self.driver_option)
        conf.save()


class Table:
    def __init__(self, db):
        self.db = db

    def show(self, table_names="*", limit=None):
        db = self.db

        if table_names == "*":
            table_names = db.tables()
        else:
            table_names = val_to_list(table_names)

        print("Total Tables:" + str(len(table_names)))

        for table_name in table_names:
            db.table_to_df(table_name, "*", limit) \
                .table(title=table_name)
