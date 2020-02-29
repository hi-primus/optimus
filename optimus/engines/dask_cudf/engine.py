
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
    def __init__(self, session=None, n_workers=2, threads_per_worker=4, processes=False, memory_limit='2GB',
                 verbose=False, comm=None, *args, **kwargs):
        if comm is True:
            Comm.instance = Comm()
        else:
            Comm.instance = comm

        self.engine = 'dask-cudf'

        # self.create = Create()
        self.load = Load()
        self.verbose(verbose)

        if session is None:
            DaskCUDF.instance = DaskCUDF().create(*args, **kwargs)
            # n_workers = n_workers, threads_per_worker = threads_per_worker,
            # processes = processes, memory_limit = memory_limit
        else:
            DaskCUDF.instance = DaskCUDF().load(session)

        # Reference https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers
        self.client = DaskCUDF.instance

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

    @property
    def spark(self):
        """
        Return a Spark session object
        :return:
        """
        return Spark.instance.spark