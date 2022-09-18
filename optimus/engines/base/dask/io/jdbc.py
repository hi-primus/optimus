import connectorx as cx
from sqlalchemy import create_engine, text, select

# Optimus plays defensive with the number of rows to be retrieved from the server so if a limit is not specified it will
# only will retrieve the LIMIT value
from optimus.engines.base.constants import NUM_PARTITIONS, TABLE_LIMIT
from optimus.engines.base.io.driver_context import DriverContext
from optimus.engines.base.io.factory import DriverFactory
from optimus.engines.base.io.properties import DriverProperties
from optimus.engines.base.meta import Meta
from optimus.helpers.core import val_to_list
from optimus.helpers.logger import logger


class DaskBaseJDBC:
    """
    Helper for JDBC connections and queries
    """

    def __init__(self, host, database, user, password, port=None, driver=None, schema="public", oracle_tns=None,
                 oracle_service_name=None, oracle_sid=None, presto_catalog=None, cassandra_keyspace=None,
                 cassandra_table=None, bigquery_project=None, bigquery_dataset=None, op=None, sso=False):

        """
        Create the JDBC connection object
        :return:
        """

        self.op = op

        if host is None:
            host = "127.0.0.1"
            # RaiseIt.value_error(host, "host must be specified")

        if user is None:
            user = "root"
            # print("user",user)
            # RaiseIt.value_error(user, "user must be specified")

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

        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        logger.print(self.uri)

    def table_names(self, schema=None, database=None, limit=None):
        """
        Return all the tables in a database
        :return:
        """
        engine = create_engine(self.uri)
        result = engine.table_names()
        engine.dispose()
        return result

    def tables(self, tables: list = None, limit=TABLE_LIMIT):
        """
        Print n rows of every table in a database
        :return: Table Object
        """
        db = self
        if tables is None:
            table_names = db.tables()
        else:
            table_names = tables

        for table_name in table_names:
            q = self._compile(select(text("*")).select_from(text(table_name)).limit(limit))
            db.execute(q, title=table_name).display()

    def desc(self):
        """
        Return a description of the database. Table names, columns names and other metadata
        :return: dict
        """
        import sqlalchemy as db

        engine = db.create_engine(self.uri)
        from sqlalchemy import inspect
        inspector = inspect(engine)

        result = {}
        for table_name in inspector.get_table_names():
            result[table_name] = inspector.get_columns(table_name)
        return result

    def _compile(self, query):
        """
        Compile a SQLAlchemy query to string
        :param query: SQLAlchemy query
        :return: String
        """
        engine = create_engine(self.uri)
        query = str(query.compile(engine, compile_kwargs={"literal_binds": True}))
        return query

    def table_to_df(self, table_name: str, columns="*", partition_on=None, limit=TABLE_LIMIT, partition_num=1):
        """
        Return cols as Spark data frames from a specific table
        :type table_name: object
        :param columns: Columns to be retrieved
        :param partition_on:, Column to partition the data
        :param partition_num: Number of partitions
        :param limit: Number of rows to be retrieved
        """

        if columns == "*":
            columns_sql = "*"
        else:
            columns = val_to_list(columns)
            columns_sql = ",".join(columns)

        query = select(text(columns_sql)).select_from(text(table_name))
        if (limit != "all") and (limit != -1):
            query = query.limit(limit)
        query = self._compile(query)

        logger.print(query)
        df = self.execute(query, partition_on=partition_on, partition_num=partition_num, title=table_name)
        # df.meta = Meta.set(df.meta, value={"name": table_name})

        return df

    def execute(self, query, partition_on: str = None, partition_num: int = NUM_PARTITIONS, limit=TABLE_LIMIT,
                title=None):
        """
        Execute a SQL query
        :param query: SQL query string
        :param partition_num: Number of partitions
        :param partition_on: Colum to partition the data
        :return: DataFrame
        """

        # Play defensive with the number of rows to be retrieved from the server so if a limit is not specified it will
        # query = f"{query} LIMIT {limit}"

        dfd = cx.read_sql(self.uri, query=query, partition_on=partition_on, partition_num=partition_num,
                          return_type="pandas")

        df = self.op.create.dataframe(dfd)
        if title is None:
            title = query
        df.meta = Meta.set(df.meta, value={"name": title})

        return df

    def df_to_table(self, df, table, mode="overwrite"):
        """
        Sends a dataframe to the database
        :param df:
        :param table:
        :param mode
        :return:
        """
        # Parse array and vector to string. JDBC can not handle this data types
        columns = df.cols.names("*", by_dtypes=["array", "vector"])
        df = df.cols.cast(columns, "str")

        conf = df.write \
            .format("jdbc" if not self.db_driver == DriverProperties.CASSANDRA.value["name"] else
                    DriverProperties.CASSANDRA.value["java_class"]) \
            .mode(mode) \
            .option("url", self.uri) \
            .option("dbtable", table) \
            .option("user", self.user) \
            .option("password", self.password)

        if self.db_driver == DriverProperties.ORACLE.value["name"]:
            conf.option("driver", self.driver_option)
        conf.save()
