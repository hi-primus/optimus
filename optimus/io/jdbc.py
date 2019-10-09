from pyspark.sql import DataFrame

from optimus.helpers.converter import val_to_list
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.io.driver import DriverResolver
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

        self.db_driver = driver
        self.oracle_sid = oracle_sid
        self.cassandra_keyspace = cassandra_keyspace
        self.cassandra_table = cassandra_table

        # Handle the default port
        if self.db_driver == DriverResolver.REDSHIFT.__str__():
            if port is None: self.port = DriverResolver.REDSHIFT.port()
            # "com.databricks.spark.redshift"

        elif self.db_driver == DriverResolver.POSTGRES_SQL.__str__():
            if port is None: self.port = DriverResolver.POSTGRES_SQL.port()
            self.driver_option = DriverResolver.POSTGRES_SQL.java_class()

        elif self.db_driver == DriverResolver.POSTGRES.__str__():  # backward compat
            if port is None: self.port = DriverResolver.POSTGRES.port()
            self.driver_option = DriverResolver.POSTGRES.java_class()
            self.db_driver = DriverResolver.POSTGRES_SQL.__str__()

        elif self.db_driver == DriverResolver.MY_SQL.__str__():
            if port is None: self.port = DriverResolver.MY_SQL.port()
            # "com.mysql.jdbc.Driver"

        elif self.db_driver == DriverResolver.SQL_SERVER.__str__():
            if port is None: self.port = DriverResolver.SQL_SERVER.port()
            # "com.microsoft.jdbc.sqlserver.SQLServerDriver"

        elif self.db_driver == DriverResolver.ORACLE.__str__():
            if port is None: self.port = DriverResolver.ORACLE.port()
            self.driver_option = DriverResolver.ORACLE.java_class()

        elif self.db_driver == DriverResolver.PRESTO.__str__():
            if port is None: self.port = DriverResolver.PRESTO.port()
            self.driver_option = DriverResolver.PRESTO.java_class()

        elif database == DriverResolver.CASSANDRA.__str__():
            # When using Cassandra there is no jdbc url since we are going to use the spark cassandra connector
            pass

        # TODO: add mongo?
        else:
            # print("Driver not supported")
            RaiseIt.value_error(driver, [database["name"] for database in DriverResolver.list()])

        port = self.port
        if database is None:
            database = ""

        # Create string connection
        url = ""
        if self.db_driver == DriverResolver.SQL_LITE.__str__():
            url = "jdbc:{DB_DRIVER}://{HOST}/{DATABASE}".format(DB_DRIVER=driver, HOST=host, DATABASE=database)
        elif self.db_driver == DriverResolver.POSTGRES_SQL.__str__() \
                or self.db_driver == DriverResolver.REDSHIFT.__str__() \
                or self.db_driver == DriverResolver.MY_SQL.__str__():
            # url = "jdbc:" + db_type + "://" + url + ":" + port + "/" + database + "?currentSchema=" + schema
            url = "jdbc:{DB_DRIVER}://{HOST}:{PORT}/{DATABASE}?currentSchema={SCHEMA}".format(DB_DRIVER=self.db_driver,
                                                                                              HOST=host,
                                                                                              PORT=port,
                                                                                              DATABASE=database,
                                                                                              SCHEMA=schema)

        elif self.db_driver == DriverResolver.ORACLE.__str__():
            if oracle_sid:
                url = "jdbc:{DB_DRIVER}:thin:@{HOST}:{PORT}/{ORACLE_SID}".format(
                    DB_DRIVER=driver,
                    HOST=host,
                    PORT=port,
                    DATABASE=database,
                    ORACLE_SID=oracle_sid,
                    SCHEMA=schema)
            elif oracle_service_name:
                url = "jdbc:{DB_DRIVER}:thin:@//{HOST}:{PORT}/{ORACLE_SERVICE_NAME}".format(DB_DRIVER=driver,
                                                                                            HOST=host,
                                                                                            PORT=port,
                                                                                            DATABASE=database,
                                                                                            ORACLE_SERVICE_NAME=oracle_service_name)

            elif oracle_tns:
                url = "jdbc:{DB_DRIVER}:thin:@//{TNS}".format(DB_DRIVER=driver, TNS=oracle_tns)

        elif self.db_driver == DriverResolver.PRESTO.__str__():
            url = "jdbc:{DB_DRIVER}://{HOST}:{PORT}/{CATALOG}/{DATABASE}".format(
                DB_DRIVER=self.db_driver,
                HOST=host,
                PORT=port,
                CATALOG=presto_catalog,
                DATABASE=database
            )

        logger.print(url)

        self.url = url
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema

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

        query = None
        if (self.db_driver == DriverResolver.REDSHIFT.__str__()) or (
                self.db_driver == DriverResolver.POSTGRES_SQL.__str__()):
            query = """
            SELECT relname as table_name,cast (reltuples as integer) AS count 
            FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) 
            WHERE nspname IN ('""" + schema + """') AND relkind='r' ORDER BY reltuples DESC"""
        elif self.db_driver == DriverResolver.SQL_SERVER.__str__():
            query = "SELECT * FROM INFORMATION_SCHEMA.TABLES"

        elif self.db_driver == DriverResolver.MY_SQL.__str__():
            query = "SELECT table_name, table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "'"

        elif self.db_driver == DriverResolver.PRESTO.__str__():
            query = "SELECT table_name, 0 as table_rows FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '" + database + "'"

        elif self.db_driver == DriverResolver.SQL_LITE.__str__():
            query = ""

        elif self.db_driver == DriverResolver.ORACLE.__str__():
            query = """SELECT table_name, 
                extractvalue(xmltype( dbms_xmlgen.getxml('select count(*) c from '||table_name)) ,'/ROWSET/ROW/C') count 
                    FROM user_tables ORDER BY table_name"""

        df = self.execute(query, limit)
        return df.table(limit)

    def tables_names_to_json(self, schema=None):
        """
        Get the table names from a database in json format
        :return:
        """

        # Override the schema used in the constructors
        if schema is None:
            schema = self.schema

        query = None
        if (self.db_driver == DriverResolver.REDSHIFT.__str__()) or (
                self.db_driver == DriverResolver.POSTGRES_SQL):
            query = """
                        SELECT relname as table_name 
                        FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) 
                        WHERE nspname IN ('""" + schema + """') AND relkind='r' ORDER BY reltuples DESC"""

        elif self.db_driver == DriverResolver.MY_SQL.__str__():
            query = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" \
                    + self.database + "' GROUP BY TABLE_NAME ORDER BY count DESC"

        elif self.db_driver == DriverResolver.SQL_LITE.__str__():
            query = ""

        elif self.db_driver == DriverResolver.ORACLE.__str__():
            query = "SELECT table_name as 'table_name' FROM user_tables"

        df = self.execute(query, "all")
        return [i['table_name'] for i in df.to_json()]

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
        if limit == "all":
            if self.db_driver == DriverResolver.ORACLE.__str__():
                query = "SELECT COUNT(*) COUNT FROM " + db_table
                count = self.execute(query, "all").to_json()[0]["COUNT"]
            else:
                query = "SELECT COUNT(*) as COUNT FROM " + db_table
                count = self.execute(query, "all").first()[0]

            # We want to count the number of rows to warn the users how much it can take to bring the whole data

            print(str(count) + " rows")

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
        if self.db_driver == DriverResolver.ORACLE.__str__():
            query = "(" + query + ") t"
        elif self.db_driver == DriverResolver.PRESTO.__str__():
            query = "(" + query + ")"
        elif self.db_driver == DriverResolver.CASSANDRA.__str__():
            query = query
        else:
            query = "(" + query + ") AS t"

        logger.print(query)
        logger.print(self.url)

        conf = Spark.instance.spark.read \
            .format("jdbc" if not self.db_driver == DriverResolver.CASSANDRA.__str__() else "org.apache.spark.sql.cassandra") \
            .option("url", self.url) \
            .option("user", self.user) \
            .option("dbtable", query)

        if self.db_driver != DriverResolver.PRESTO.__str__() and self.password is not None:
            conf.option("password", self.password)

        if self.db_driver == DriverResolver.ORACLE.__str__() \
                or self.db_driver == DriverResolver.POSTGRES_SQL.__str__() \
                or self.db_driver == DriverResolver.PRESTO.__str__():
            conf.option("driver", self.driver_option)

        if self.db_driver == DriverResolver.CASSANDRA.__str__():
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
            .format("jdbc" if not DriverResolver.CASSANDRA.__str__() else "org.apache.spark.sql.cassandra") \
            .mode(mode) \
            .option("url", self.url) \
            .option("dbtable", table) \
            .option("user", self.user) \
            .option("password", self.password)

        if self.db_driver == "oracle":
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
