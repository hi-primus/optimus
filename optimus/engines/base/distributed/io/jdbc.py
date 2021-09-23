import numpy as np
import pandas as pd
import sqlalchemy as sa
from dask.dataframe import from_delayed, from_pandas
from dask.delayed import delayed
from sqlalchemy import create_engine
from sqlalchemy import sql
from sqlalchemy.sql import elements

# Optimus plays defensive with the number of rows to be retrieved from the server so if a limit is not specified it will
# only will retrieve the LIMIT value
from optimus.engines.base.constants import NUM_PARTITIONS, LIMIT_TABLE
from optimus.engines.base.io.driver_context import DriverContext
from optimus.engines.base.io.factory import DriverFactory
from optimus.engines.base.io.properties import DriverProperties
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
        # return df.display(limit)

        engine = create_engine(self.uri)
        return engine.table_names()

    @property
    def table(self):
        """
        Print n rows of every table in a database
        :return: Table Object
        """
        return Table(self)

    def table_to_df(self, table_name: str, columns="*", limit=None):
        """
        Return cols as Spark data frames from a specific table
        :type table_name: object
        :param columns:
        :param limit: how many rows will be retrieved
        """
        db_table = table_name

        if limit == "all":
            query = self.driver_context.count_query(db_table=db_table)
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

        dfd = self.execute(query, limit)
        # Bring the data to local machine if not every time we call an action is going to be
        # retrieved from the remote server
        # dfd = dfd.run()
        # dfd = dask_pandas_to_dask_cudf(dfd)
        return self.op.create.dataframe(self.op.F.dask_to_compatible(dfd))

    def execute(self, query, limit=None, num_partitions: int = NUM_PARTITIONS, partition_column: str = None,
                table_name=None):
        """
        Execute a SQL query
        :param limit: default limit the whole query. We play defensive here in case the result is a big chunk of data
        :param num_partitions:
        :param partition_column:
        :param query: SQL query string
        :param table_name:
        :return:
        """

        # play defensive with a select clause
        # if self.db_driver == DriverProperties.ORACLE.value["name"]:
        #     query = "(" + query + ") t"
        # elif self.db_driver == DriverProperties.PRESTO.value["name"]:
        #     query = "(" + query + ")"
        # elif self.db_driver == DriverProperties.CASSANDRA.value["name"]:
        #     query = query
        # else:
        #     query = "(" + query + ") AS t"

        # df = dd.read_sql_table(table='test_data', uri=self.url, index_col='id')
        # "SELECT table_name, table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'optimus'"
        df = DaskBaseJDBC.read_sql_table(table_name=table_name, uri=self.uri, index_col=partition_column,
                                         npartitions=num_partitions, query=query)
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

    @staticmethod
    def read_sql_table(
            table_name,
            uri,
            index_col=None,
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

        engine_kwargs = {} if engine_kwargs is None else engine_kwargs
        engine = sa.create_engine(uri, **engine_kwargs)
        m = sa.MetaData()

        # print("table", table)
        if isinstance(table_name, str):
            table_name = sa.Table(table_name, m, autoload=True, autoload_with=engine, schema=schema)

            columns = (
                [(table_name.columns[c] if isinstance(c, str) else c) for c in columns]
                if columns
                else list(table_name.columns)
            )
        index = None
        if index_col:
            # raise ValueError("Must specify index column to partition on")
            # else:

            index = table_name.columns[index_col] if isinstance(index_col, str) else index_col
            if not isinstance(index_col, (str, elements.Label)):
                raise ValueError(
                    "Use label when passing an SQLAlchemy instance as the index (%s)" % index
                )
            if divisions and npartitions:
                raise TypeError("Must supply either divisions or npartitions, not both")

            if index_col not in columns:
                columns.append(
                    table_name.columns[index_col] if isinstance(index_col, str) else index_col
                )

            if isinstance(index_col, str):
                kwargs["index_col"] = index_col
            else:
                # function names get pandas auto-named
                kwargs["index_col"] = index_col.name
        parts = []

        if meta is None:
            # derive metadata from first few rows
            # q = sql.select(columns).limit(head_rows).select_from(table)
            head = pd.read_sql(query, engine, **kwargs)
            # print("head", head)
            # print("META", head.iloc[:0])
            if head.empty:
                # no results at all
                # name = table_name.name
                # schema = table_name.schema
                # head = pd.read_sql_table(name, uri, schema=schema, index_col=index_col)

                return from_pandas(head, npartitions=1)

            bytes_per_row = (head.memory_usage(deep=True, index=True)).sum() / head_rows
            meta = head.iloc[:0]
            # print(list(head.columns.values))
        else:
            if divisions is None and npartitions is None:
                raise ValueError(
                    "Must provide divisions or npartitions when using explicit meta."
                )

        # if divisions is None and index_col is not None:
        if divisions is None:
            # print(index)
            # print("LIMITS",limits)
            if index is not None and limits is None:
                # calculate max and min for given index
                q = sql.select([sql.func.max(index), sql.func.min(index)]).select_from(
                    table_name
                )
                minmax = pd.read_sql(q, engine)
                maxi, mini = minmax.iloc[0]
                dtype = minmax.dtypes["max_1"]
            elif index is None and npartitions:
                # User for Limit offset
                mini = 0
                q = f"SELECT COUNT(*) AS count FROM ({query}) AS query"
                maxi = pd.read_sql(q, engine)["count"][0]
                limit = maxi / npartitions
                dtype = pd.Series((mini, maxi,)).dtype

                #  Use for ntile calculation
                # mini = 0
                # maxi = npartitions
                # print(pd.Series((mini, maxi,)))
                # dtype = pd.Series((mini, maxi,)).dtype
                # ntile_columns = ", ".join(list(head.columns.values))

            else:
                mini, maxi = limits
                dtype = pd.Series(limits).dtype

            if npartitions is None:
                q = sql.select([sql.func.count(index)]).select_from(table_name)
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
            # else:
            #     print(dtype)
            #     raise TypeError(
            #         'Provided index column is of type "{}".  If divisions is not provided the '
            #         "index column type must be numeric or datetime.".format(dtype)
            #     )

            lowers, uppers = divisions[:-1], divisions[1:]
            for i, (lower, upper) in enumerate(zip(lowers, uppers)):

                if index_col:
                    if i == len(lowers) - 1:
                        where = f" WHERE {index} > {lower} AND {index} <= {upper}"
                    else:
                        where = f" WHERE {index} >= {lower} AND {index} < {upper}"
                    q = query + where
                else:
                    if i == len(lowers) - 1:
                        where = f" LIMIT {int(upper) - int(lower)} OFFSET {int(lower)}"
                    else:
                        where = f" LIMIT {int(limit)} OFFSET {int(lower)}"
                    q = query + where

                    # Ntile calculation
                    # print("Using Ntile query")
                    # ntile_column = "temp"
                    # ntile_sql = f"SELECT *, NTILE({npartitions}) OVER(ORDER BY id DESC) AS {ntile_column} FROM ({query}) AS t";
                    # q = f"SELECT {ntile_columns} FROM ({ntile_sql}) AS r"
                    # if i == len(lowers) - 1:
                    #     where = f" WHERE {ntile_column} > {lower} AND {ntile_column} <= {upper}"
                    # else:
                    #     where = f" WHERE {ntile_column} >= {lower} AND {ntile_column} < {upper}"
                    # q = q + where

                    # table = "test_data"
                    # q = f'SELECT {ntile_columns} FROM {table} WHERE  NTILE({npartitions}) OVER (ORDER BY {ntile_column}) = i'

                # When we do not have and index
                parts.append(
                    delayed(DaskBaseJDBC._read_sql_chunk)(
                        q, uri, meta, engine_kwargs=engine_kwargs, **kwargs
                    )
                )
        else:

            # JDBC._read_sql_chunk(q, uri, meta, engine_kwargs=engine_kwargs, **kwargs)

            parts.append(
                delayed(DaskBaseJDBC._read_sql_chunk)(
                    query, uri, meta, engine_kwargs=engine_kwargs, **kwargs
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
            # df.reset_index()
            return df.astype(meta.dtypes.to_dict(), copy=False)

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
            .format(
            "jdbc" if not self.db_driver == DriverProperties.CASSANDRA.value["name"] else
            DriverProperties.CASSANDRA.value["java_class"]) \
            .mode(mode) \
            .option("url", self.uri) \
            .option("dbtable", table) \
            .option("user", self.user) \
            .option("password", self.password)

        if self.db_driver == DriverProperties.ORACLE.value["name"]:
            conf.option("driver", self.driver_option)
        conf.save()

    @staticmethod
    def _limit(df, limit=None):
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

        if table_names == "*":
            table_names = db.tables()
        else:
            table_names = val_to_list(table_names)

        print("Total Tables:" + str(len(table_names)))

        for table_name in table_names:
            db.table_to_df(table_name, "*", limit) \
                .table(title=table_name)
