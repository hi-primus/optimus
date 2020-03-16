from dask.distributed import Client

from optimus.bumblebee import Comm
from optimus.engines.dask.dask import Dask
from optimus.engines.dask.io.jdbc import JDBC
from optimus.engines.dask.io.load import Load
from optimus.helpers.logger import logger
from optimus.profiler.profiler import Profiler
from optimus.version import __version__

Dask.instance = None
Profiler.instance = None
Comm.instance = None


class DaskEngine:
    __version__ = __version__

    def __init__(self, session=None, n_workers=2, threads_per_worker=4, processes=False, memory_limit='2GB',
                 verbose=False, comm=None, *args, **kwargs):
        if comm is True:
            Comm.instance = Comm()
        else:
            Comm.instance = comm

        self.engine = 'dask'

        # self.create = Create()
        self.load = Load()
        self.verbose(verbose)

        if session is None:
            Dask.instance = Client(*args, **kwargs)
            # a = Dask()
            # b = a.create(n_workers=n_workers, threads_per_worker=threads_per_worker,
            #              processes=processes, memory_limit=memory_limit, *args, **kwargs)
            Dask.instance = b
        else:
            Dask.instance = Dask().load(session)

        # Reference https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers
        self.client = Dask.instance

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

    @property
    def dask(self):
        """
        Return a Spark session object
        :return:
        """
        return Dask.instance.dask

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
    # def create(self, data):
    #     import dask.dataframe as dd
    #     return dd.DataFrame(data)
