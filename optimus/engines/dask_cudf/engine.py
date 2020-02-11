from dask.distributed import Client
from dask_cuda import LocalCUDACluster

from optimus.bumblebee import Comm
from optimus.engines.dask_cudf.io.jdbc import JDBC
from optimus.engines.dask_cudf.dask_cudf import DaskCUDF
from optimus.engines.dask_cudf.io.load import Load
from optimus.helpers.logger import logger
from optimus.profiler.profiler import Profiler

DaskCUDF.instance = None
Profiler.instance = None
Comm.instance = None


class DaskCUDFEngine:
    def __init__(self, verbose=False, comm=None, *args, **kwargs):
        # self.create = Create()
        self.load = Load()
        # self.read = self.spark.read

        cluster = LocalCUDACluster()
        client = Client(cluster)

        self.engine = 'dask_cudf'
        # self.create = Create()
        self.load = Load()
        # self.read = self.spark.read
        self.verbose(verbose)

        DaskCUDF.instance = client
        self.client = client

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance

    @staticmethod
    def verbose(verbose):
        """
        Enable verbose mode
        :param verbose:
        :return:
        """

        logger.active(verbose)

    @staticmethod
    def connect(driver=None, host=None, database=None, user=None, password=None, port=None, schema="public",
                oracle_tns=None, oracle_service_name=None, oracle_sid=None, presto_catalog=None,
                cassandra_keyspace=None, cassandra_table=None):
        """
        Create the JDBC string connection
        :return: JDBC object
        """

        return JDBC(host, database, user, password, port, driver, schema, oracle_tns, oracle_service_name, oracle_sid,
                        presto_catalog, cassandra_keyspace, cassandra_table)
