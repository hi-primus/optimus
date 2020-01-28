from dask.distributed import Client
from dask_cuda import LocalCUDACluster

from optimus.bumblebee import Comm
from optimus.engines.dask.dask import Dask
from optimus.engines.dask.io.load import Load
from optimus.helpers.logger import logger
from optimus.profiler.profiler import Profiler

Dask.instance = None
Profiler.instance = None
Comm.instance = None


class DaskCUDFEngine:
    def __init__(self, verbose=False, comm=None, *args, **kwargs):
        self.engine = 'dask_cudf'
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

        Dask.instance = client

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
