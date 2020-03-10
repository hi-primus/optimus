import pandas as pd

from optimus.bumblebee import Comm
from optimus.engines.cudf.io.jdbc import JDBC
from optimus.engines.cudf.io.load import Load
from optimus.engines.cudf.cudf import CUDF
from optimus.helpers.logger import logger
from optimus.profiler.profiler import Profiler
from optimus.version import __version__

CUDF.instance = None
Profiler.instance = None
Comm.instance = None


class CUDFEngine:
    __version__ = __version__

    def __init__(self, verbose=False, comm=None, *args, **kwargs):
        if comm is True:
            Comm.instance = Comm()
        else:
            Comm.instance = comm

        self.engine = 'cudf'
        # self.create = Create()
        self.load = Load()
        # self.read = self.spark.read
        self.verbose(verbose)

        CUDF.instance = pd

        self.client = CUDF.instance

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
