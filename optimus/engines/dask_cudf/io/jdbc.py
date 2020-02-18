from optimus.engines.base.contants import NUM_PARTITIONS
from optimus.engines.base.dask.io.jdbc import DaskBaseJDBC
from optimus.engines.dask_cudf.helpers import dask_pandas_to_cudf, dask_pandas_to_dask_cudf


class JDBC(DaskBaseJDBC):
    def __init__(self, host, database, user, password, port=None, driver=None, schema="public", oracle_tns=None,
                 oracle_service_name=None, oracle_sid=None, presto_catalog=None, cassandra_keyspace=None,
                 cassandra_table=None):
        super().__init__(host, database, user, password, port=port, driver=driver, schema=schema,
                         oracle_tns=oracle_tns,
                         oracle_service_name=oracle_service_name, oracle_sid=oracle_sid,
                         presto_catalog=presto_catalog,
                         cassandra_keyspace=cassandra_keyspace,
                         cassandra_table=cassandra_table)

    def table_to_df(self, table_name, columns="*", limit=None):
        # print(type(super().table_to_df(table_name, columns, limit)))
        return super().table_to_df(table_name, columns, limit)
        # return super().table_to_df(table_name, columns, limit)


    def table_to_dask_cudf(self, table_name, columns="*", limit=None):
        print("TABLE")
        # return dask_pandas_to_dask_cudf(super().table_to_df(table_name, columns, limit))
        a = dask_pandas_to_dask_cudf(super().table_to_df(table_name, columns, limit))
        print(type(a))
        return a

    # def execute(self, query, limit=None, num_partitions: int = NUM_PARTITIONS, partition_column: str = None,
    #             table_name=None):
    #     return super().execute(query, limit, num_partitions, partition_column, table_name).compute()
