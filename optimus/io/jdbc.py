from pyspark.sql import DataFrame

from optimus.helpers.converter import val_to_list
from optimus.helpers.logger import logger
from optimus.io.driver_context import DriverContext
from optimus.io.factory import DriverFactory
from optimus.io.properties import DriverProperties
from optimus.spark import Spark

# Optimus play defensive with the number of rows to be retrieved from the server so if a limit is not specified it will
# only will retrieve the LIMIT value
LIMIT = 1000
LIMIT_TABLE = 10


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
        return df.table(limit)

    def tables_names_to_json(self, schema=None):
        """
        Get the table names from a database in json format
        :return:
        """

        # Override the schema used in the constructors
        if schema is None: schema = self.schema
        query = self.driver_context.table_name_query(schema=schema, database=self.database)
        table_name = self.driver_properties.value["table_name"]
        df = self.execute(query, "all")
        return [i[table_name] for i in df.to_dict()]

    @property
    def table(self):
        """
        Print n rows of every table in a database
        :return: Table Object
        """
        return Table(self)

    def table_to_df(self, table_name, columns="*", limit=None):
        """
        Return cols as Spark dataframe from a specific table
        :type table_name: object
        :param columns:
        :param limit: how many rows will be retrieved
        """

        db_table = table_name
        query = self.driver_context.count_query(db_table=db_table)
        if limit == "all":
            count = self.execute(query, "all").first()[0]

            # We want to count the number of rows to warn the users how much it can take to bring the whole data
            print(str(int(count)) + " rows")

        if columns == "*":
            columns_sql = "*"
        else:
            columns = val_to_list(columns)
            columns_sql = ",".join(columns)

        query = "SELECT " + columns_sql + " FROM " + db_table

        logger.print(query)
        df = self.execute(query, limit)

        # Bring the data to local machine if not every time we call an action is going to be
        # retrieved from the remote server
        df = df.run()
        return df

    def execute(self, query, limit=None):
        """
        Execute a SQL query
        :param limit: default limit the whole query. We play defensive here in case the result is a big chunk of data
        :param query: SQL query string
        :return:
        """

        # play defensive with a select clause
        if self.db_driver == DriverProperties.ORACLE.value["name"]:
            query = "(" + query + ") t"
        elif self.db_driver == DriverProperties.PRESTO.value["name"]:
            query = "(" + query + ")"
        elif self.db_driver == DriverProperties.CASSANDRA.value["name"]:
            query = query
        else:
            query = "(" + query + ") AS t"

        logger.print(query)
        logger.print(self.url)

        conf = Spark.instance.spark.read \
            .format(
            "jdbc" if not self.db_driver == DriverProperties.CASSANDRA.value["name"] else
            DriverProperties.CASSANDRA.value["java_class"]) \
            .option("url", self.url) \
            .option("user", self.user) \
            .option("dbtable", query)

        # Password
        if self.db_driver != DriverProperties.PRESTO.value["name"] and self.password is not None:
            conf.option("password", self.password)

        # Driver
        if self.db_driver == DriverProperties.ORACLE.value["name"] \
                or self.db_driver == DriverProperties.POSTGRESQL.value["name"] \
                or self.db_driver == DriverProperties.PRESTO.value["name"]:
            conf.option("driver", self.driver_option)

        if self.db_driver == DriverProperties.CASSANDRA.value["name"]:
            conf.options(table=self.cassandra_table, keyspace=self.cassandra_keyspace)

        return self._limit(conf.load(), limit)

    def df_to_table(self, df, table, mode="overwrite"):
        """
        Send a dataframe to the database
        :param df:
        :param table:
        :param mode
        :return:
        """
        # Parse array and vector to string. JDBC can not handle this data types
        columns = df.cols.names("*", filter_by_column_dtypes=["array", "vector"])
        df = df.cols.cast(columns, "str")

        conf = df.write \
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

    @staticmethod
    def _limit(df: DataFrame, limit=None):
        """
        Handle limit defensive so we do not retrieve the whole at we explicit want
        :param limit:
        :param df:
        :return a limited DataFrame if specified
        """
        # we use a default limit here in case the query will return a huge chunk of data
        if limit is None:
            return df.limit(LIMIT_TABLE)
        elif limit == "all":
            return df
        else:
            return df.limit(int(limit))


class Table:
    def __init__(self, db):
        self.db = db

    def show(self, table_names="*", limit=None):
        db = self.db

        if table_names is "*":
            table_names = db.tables_names_to_json()
        else:
            table_names = val_to_list(table_names)

        print("Total Tables:" + str(len(table_names)))

        for table_name in table_names:
            db.table_to_df(table_name, "*", limit) \
                .table(title=table_name)
