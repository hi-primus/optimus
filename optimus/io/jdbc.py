import humanize

from optimus.helpers.raiseit import RaiseIt
from optimus.spark import Spark


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

    def tables(self):
        """
        Return all the tables in a database
        :return:
        """
        query = None
        if (self.db_type is "redshift") or (self.db_type is "postgres"):
            query = """
            (SELECT relname as table_name,cast (reltuples as integer) AS count 
            FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) 
            WHERE nspname NOT IN ('pg_catalog', 'information_schema') AND relkind='r' ORDER BY reltuples DESC) as t"""

        elif self.db_type is "mysql":
            query = "(SELECT TABLE_NAME AS table_name, SUM(TABLE_ROWS) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" \
                    + self.database + "' GROUP BY TABLE_NAME ORDER BY count DESC ) as t"

        elif self.db_type is "sqlite":
            query = ""

        # print(query)
        df = self.execute(query)
        df.table()

    def table_to_df(self, table_name, columns="*", limit=None):
        """
        Return cols as Spark dataframe from a specific table
        """

        # We want to count the number of rows to warn the users how much it can take to bring the whole data
        db_table = "public." + table_name
        if limit is None:
            query = "(SELECT COUNT(*) FROM " + db_table + ") as t"
            count = self.execute(query).to_json()[0]["count"]
        else:
            count = limit

        print(humanize.intword(count) + " rows in *" + table_name + "* table")

        if columns is "*":
            columns_sql = "*"
        else:
            columns_sql = ",".join(columns)

        if limit is None:
            query = "(SELECT " + columns_sql + " FROM " + db_table + ") AS t"
        else:
            query = "(SELECT " + columns_sql + "  FROM " + db_table + " LIMIT " + str(limit) + ") AS t"

        df = self.execute(query)

        # Bring the data to local machine if not every time we call an action is going to be
        # retrived from the remote server
        df = df.run()
        return df

    def execute(self, query):
        """
        Execute a SQL query
        :param query: SQL query string
        :return:
        """
        # query = "(SELECT * FROM " + table_name + " LIMIT 10) AS t"
        return Spark.instance.spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", query) \
            .option("user", self.user) \
            .option("password", self.password) \
            .load()
