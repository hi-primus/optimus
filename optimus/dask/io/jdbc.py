import numpy as np
import pandas as pd
from dask.dataframe import from_delayed, from_pandas
from dask.delayed import delayed
from pyspark.sql import DataFrame
from sqlalchemy import create_engine

from optimus.helpers.converter import val_to_list
from optimus.helpers.logger import logger
from optimus.spark.io.driver_context import DriverContext
from optimus.spark.io.factory import DriverFactory
from optimus.spark.io.properties import DriverProperties

# Optimus plays defensive with the number of rows to be retrieved from the server so if a limit is not specified it will
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
        # if database is None:
        #     database = self.database
        #
        # if schema is None:
        #     schema = self.schema
        # query = self.driver_context.table_names_query(schema=schema, database=database)
        # df = self.execute(query, limit)
        # return df.ext.display(limit)

        engine = create_engine(self.url)
        print(engine.table_names())

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
        return [i[table_name] for i in df.ext.to_dict()]

    @property
    def table(self):
        """
        Print n rows of every table in a database
        :return: Table Object
        """
        return Table(self)

    def table_to_df(self, table_name, columns="*", limit=None):
        """
        Return cols as Spark data frames from a specific table
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

        print(query)
        print(self.url)

        # df = dd.read_sql_table(table='test_data', uri=self.url, index_col='id')
        # "SELECT table_name, table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'optimus'"
        df = JDBC.read_sql_table(table='test_data', uri=self.url, index_col=None,
                                 query="SELECT * FROM test_data")

        # print(len(df))

        # conf = Spark.instance.spark.read \
        #     .format(
        #     "jdbc" if not self.db_driver == DriverProperties.CASSANDRA.value["name"] else
        #     DriverProperties.CASSANDRA.value["java_class"]) \
        #     .option("url", self.url) \
        #     .option("user", self.user) \
        #     .option("dbtable", query)
        #
        # # Password
        # if self.db_driver != DriverProperties.PRESTO.value["name"] and self.password is not None:
        #     conf.option("password", self.password)
        #
        # # Driver
        # if self.db_driver == DriverProperties.ORACLE.value["name"] \
        #         or self.db_driver == DriverProperties.POSTGRESQL.value["name"] \
        #         or self.db_driver == DriverProperties.PRESTO.value["name"]:
        #     conf.option("driver", self.driver_option)
        #
        # if self.db_driver == DriverProperties.CASSANDRA.value["name"]:
        #     conf.options(table=self.cassandra_table, keyspace=self.cassandra_keyspace)

        # return self._limit(conf.load(), limit)
        return df

    def df_to_table(self, df, table, mode="overwrite"):
        """
        Send a spark to the database
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

    @staticmethod
    def read_sql_table(
            table,
            uri,
            index_col,
            divisions=None,
            npartitions=None,
            limits=None,
            columns=None,
            bytes_per_chunk=256 * 2 ** 20,
            head_rows=5,
            schema=None,
            meta=None,
            engine_kwargs=None,
            query=None,
            **kwargs
    ):
        import sqlalchemy as sa
        from sqlalchemy import sql
        from sqlalchemy.sql import elements

        engine_kwargs = {} if engine_kwargs is None else engine_kwargs
        engine = sa.create_engine(uri, **engine_kwargs)
        m = sa.MetaData()
        if isinstance(table, str):
            table = sa.Table(table, m, autoload=True, autoload_with=engine, schema=schema)

        columns = (
            [(table.columns[c] if isinstance(c, str) else c) for c in columns]
            if columns
            else list(table.columns)
        )
        if index_col is not None:
            # raise ValueError("Must specify index column to partition on")
            # else:

            index = table.columns[index_col] if isinstance(index_col, str) else index_col
            if not isinstance(index_col, (str, elements.Label)):
                raise ValueError(
                    "Use label when passing an SQLAlchemy instance as the index (%s)" % index
                )
            if divisions and npartitions:
                raise TypeError("Must supply either divisions or npartitions, not both")

            if index_col not in columns:
                columns.append(
                    table.columns[index_col] if isinstance(index_col, str) else index_col
                )

            if isinstance(index_col, str):
                kwargs["index_col"] = index_col
            else:
                # function names get pandas auto-named
                kwargs["index_col"] = index_col.name

        if meta is None:
            # derive metadata from first few rows
            q = sql.select(columns).limit(head_rows).select_from(table)
            head = pd.read_sql(q, engine, **kwargs)
            print("META", head.iloc[:0])
            if head.empty:
                # no results at all
                name = table.name
                schema = table.schema
                head = pd.read_sql_table(name, uri, schema=schema, index_col=index_col)
                return from_pandas(head, npartitions=1)

            bytes_per_row = (head.memory_usage(deep=True, index=True)).sum() / head_rows
            meta = head.iloc[:0]
        else:
            if divisions is None and npartitions is None:
                raise ValueError(
                    "Must provide divisions or npartitions when using explicit meta."
                )

        if divisions is None and index_col is not None:
            if limits is None:
                # calculate max and min for given index
                q = sql.select([sql.func.max(index), sql.func.min(index)]).select_from(
                    table
                )
                minmax = pd.read_sql(q, engine)
                maxi, mini = minmax.iloc[0]
                dtype = minmax.dtypes["max_1"]
            else:
                mini, maxi = limits
                dtype = pd.Series(limits).dtype

            if npartitions is None:
                q = sql.select([sql.func.count(index)]).select_from(table)
                count = pd.read_sql(q, engine)["count_1"][0]
                npartitions = int(round(count * bytes_per_row / bytes_per_chunk)) or 1

            if dtype.kind == "M":
                divisions = pd.date_range(
                    start=mini,
                    end=maxi,
                    freq="%iS" % ((maxi - mini).total_seconds() / npartitions),
                ).tolist()
                divisions[0] = mini
                divisions[-1] = maxi
            elif dtype.kind in ["i", "u", "f"]:
                divisions = np.linspace(mini, maxi, npartitions + 1).tolist()
            else:
                raise TypeError(
                    'Provided index column is of type "{}".  If divisions is not provided the '
                    "index column type must be numeric or datetime.".format(dtype)
                )

            parts = []
            lowers, uppers = divisions[:-1], divisions[1:]
            for i, (lower, upper) in enumerate(zip(lowers, uppers)):
                cond = index <= upper if i == len(lowers) - 1 else index < upper

                q = sql.select(columns).where(sql.and_(index >= lower, cond)).select_from(table)
                parts.append(
                    delayed(JDBC._read_sql_chunk)(
                        q, uri, meta, engine_kwargs=engine_kwargs, **kwargs
                    )
                )
        else:
            # kwargs["index_col"] = 'id'
            parts = []
            q = query
            print(q, uri, meta, kwargs)
            parts.append(
                delayed(JDBC._read_sql_chunk)(
                    q, uri, meta, engine_kwargs=engine_kwargs, **kwargs
                )
            )

        engine.dispose()

        return from_delayed(parts, meta, divisions=divisions)

    @staticmethod
    def _read_sql_chunk(q, uri, meta, engine_kwargs=None, **kwargs):
        import sqlalchemy as sa

        engine_kwargs = engine_kwargs or {}
        engine = sa.create_engine(uri, **engine_kwargs)
        df = pd.read_sql(q, engine, **kwargs)
        engine.dispose()
        if df.empty:
            return meta
        else:
            return df.astype(meta.dtypes.to_dict(), copy=False)


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
