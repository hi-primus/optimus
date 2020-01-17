from dask.distributed import Client

from optimus.bumblebee import Comm
from optimus.dask.dask import Dask
from optimus.dask.io.load import Load
from optimus.profiler.profiler import Profiler
from optimus.dask.io.jdbc import JDBC
from optimus.version import __version__

Dask.instance = None
Profiler.instance = None
Comm.instance = None


class DaskEngine:
    __version__ = __version__


    def __init__(self, *args, **kwargs):
        self.engine = 'dask'
        # self.create = Create()
        self.load = Load()
        # self.read = self.spark.read

        Dask.instance = Client(n_workers=2, threads_per_worker=4, processes=False, memory_limit='2GB')

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance

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
