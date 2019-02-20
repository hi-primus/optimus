from optimus.spark import Spark


class JDBC:
    """
    Helper for JDBC connections and queries
    """

    def __init__(self, db_type, url, database, user, password, port):
        """

        :return:
        """
        url = "jdbc:" + db_type + "://" + url + "/" + database
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

        df = self.conn(
            "(SELECT relname as table_name,cast (reltuples as integer) AS count FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) WHERE nspname NOT IN ('pg_catalog', 'information_schema') AND relkind='r' ORDER BY reltuples DESC) as t")
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
