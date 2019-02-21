from optimus.helpers.raiseit import RaiseIt
from optimus.spark import Spark


class JDBC:
    """
    Helper for JDBC connections and queries
    """

    def __init__(self, db_type, url, database, user, password, port=None):
        """

        :return:
        """


        # RaiseIt.value_error(db_type, ["redshift", "postgres", "mysql", "sqlite"])

        if port is None:
            if self.db_type is "redshift":
                self.port = 5439

            if self.db_type is "postgres":
                self.port = 5432

            elif self.db_type is "mysql":
                self.port = 3306

        url = "jdbc:" + db_type + "://" + url + "/" + database

        self.db_type = db_type
        self.url = url
        self.database = database
        self.user = user
        self.password = password
        self.port = port

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
        df = self.conn(query)
        df.table()

    def table_to_df(self, table_name, limit=100):
        """
        Return cols from a specific table
        """
        db_table = "public." + table_name
        if limit is None:
            query = "(SELECT * FROM " + db_table + ") AS t"
        else:
            query = "(SELECT * FROM " + db_table + " LIMIT " + str(limit) + ") AS t"

        return self.conn(query)

    def conn(self, query):
        # query = "(SELECT * FROM " + table_name + " LIMIT 10) AS t"

        return Spark.instance.spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", query) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("port", self.port) \
            .load()
