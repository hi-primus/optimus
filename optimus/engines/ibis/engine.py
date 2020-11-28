from optimus.engines.base.engine import BaseEngine
# from optimus.engines.dask_cudf.dask_cudf import DaskCUDF
# from optimus.engines.dask_cudf.io.load import Load
from optimus.engines.ibis.io.jdbc import JDBC
from optimus.profiler.profiler import Profiler

# DaskCUDF.instance = None
Profiler.instance = None


class IbisEngine(BaseEngine):
    # __version__ = __version__

    def __init__(self, verbose=False, comm=None, *args, **kwargs):

        self.engine = 'ibis'
        # self.create = Create(pd)
        # self.load = Load()
        # self.extract = Extract()

        self.verbose(verbose)

        # Pandas.instance = pd

        # self.client = pd

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance

    @staticmethod
    def connect(driver=None, host=None, database=None, user=None, password=None, port=None, schema="public",
                oracle_tns=None, oracle_service_name=None, oracle_sid=None, presto_catalog=None,
                cassandra_keyspace=None, cassandra_table=None, bigquery_project=None, bigquery_dataset=None):
        """
        Create the JDBC string connection
        :return: JDBC object
        """

        return JDBC(host, database, user, password, port, driver, schema, oracle_tns, oracle_service_name, oracle_sid,
                    presto_catalog, cassandra_keyspace, cassandra_table, bigquery_project, bigquery_dataset)
