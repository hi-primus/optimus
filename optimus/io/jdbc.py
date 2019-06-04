from optimus.helpers.convert import val_to_list
from optimus.helpers.logger import logger
from optimus.spark import Spark

# Optimus play defensive with the number of rows to be retrieved from the server so a limit is not specified in
# a function that required it only will retrieve the LIMIT value
LIMIT = 1000
LIMIT_TABLE = 10


class JDBC:
    """
    Helper for JDBC connections and queries
    """

    def __init__(self, db_type, url, database, user, password, port=None):
        """
        Create the JDBC connection object
        :return:
        """
        # RaiseIt.value_error(db_type, ["redshift", "postgres", "mysql", "sqlite"])
        self.db_type = db_type

        # Create string connection
        if self.db_type is "sqlite":
            url = "jdbc:" + db_type + ":" + url + "/" + database
        else:
            url = "jdbc:" + db_type + "://" + url + "/" + database

        # Handle the default port
        if port is None:
            if self.db_type is "redshift":
                self.port = 5439

            if self.db_type is "postgres":
                self.port = 5432

            elif self.db_type is "mysql":
                self.port = 3306

        self.url = url
        self.database = database
        self.user = user
        self.password = password

    def tables(self, schema='public'):
        """
        Return all the tables in a database
        :return:
        """
        query = None
        if (self.db_type is "redshift") or (self.db_type is "postgres"):
            query = """
            SELECT relname as table_name,cast (reltuples as integer) AS count 
            FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) 
            WHERE nspname IN ('""" + schema + """') AND relkind='r' ORDER BY reltuples DESC"""

        elif self.db_type is "mysql":
            query = "SELECT TABLE_NAME AS table_name, SUM(TABLE_ROWS) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" \
                    + self.database + "' GROUP BY TABLE_NAME ORDER BY count DESC"

        elif self.db_type is "sqlite":
            query = ""

        # print(query)
        df = self.execute(query, "all")
        df.table()

    def tables_names_to_json(self, schema='public'):
        """
        Get the table names from a database in json format
        :return:
        """
        query = None
        if (self.db_type is "redshift") or (self.db_type is "postgres"):
            query = """
                    SELECT relname as table_name 
                    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) 
                    WHERE nspname IN ('""" + schema + """') AND relkind='r' ORDER BY reltuples DESC"""

        elif self.db_type is "mysql":
            query = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" \
                    + self.database + "' GROUP BY TABLE_NAME ORDER BY count DESC"

        elif self.db_type is "sqlite":
            query = ""

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

        db_table = "public." + table_name
        if self._limit(limit) is "all":
            query = "SELECT COUNT(*) FROM " + db_table
            # We want to count the number of rows to warn the users how much it can take to bring the whole data
            count = self.execute(query, "all").to_json()[0]["count"]

            print(str(count) + " rows")

        if columns is "*":
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

        query = "(" + query + self._limit(limit) + ") AS t"

        logger.print(query)
        return Spark.instance.spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", query) \
            .option("user", self.user) \
            .option("password", self.password) \
            .load()

    def df_to_table(self, df, table, mode="overwrite"):
        """
        Send a dataframe to the database
        :param df:
        :param table:
        :param mode
        :return:
        """
        return df.write \
            .format("jdbc") \
            .mode(mode) \
            .option("url", self.url) \
            .option("dbtable", table) \
            .option("user", self.user) \
            .option("password", self.password) \
            .save()

    @staticmethod
    def _limit(limit=None):
        """
        Handle limit defensive so we do not retrieve the whole at we explicit want
        :param limit:
        :return:
        """
        # we use a default limit here in case the query will return a huge chunk of data
        if limit is None:
            limit_query = " LIMIT " + str(LIMIT_TABLE)
        elif limit is "all":
            limit_query = ""
        else:
            limit_query = " LIMIT " + str(limit)
        return limit_query


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
