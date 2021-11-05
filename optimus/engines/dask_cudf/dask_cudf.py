from distributed import Client
# from dask_cuda import LocalCUDACluster

from optimus.engines.base.dask.constants import STARTING_DASK
from optimus.helpers.constants import JUST_CHECKING
from optimus.helpers.logger import logger


class DaskCUDF:
    def __init__(self):
        self._dask = None
        self._client = None
        self._cluster = None

    def create(self, *args, **kwargs):
        logger.print(JUST_CHECKING)
        logger.print("-----")

        logger.print(STARTING_DASK)

        cluster = LocalCUDACluster()
        self._client = Client(cluster, *args, **kwargs)
        self._cluster = cluster

        return self

    def load(self, session):
        self._client = session
        return self

    @property
    def dask(self):
        return self._client

    def compute(self, *args, **kwargs):
        return self._client.compute(*args, **kwargs)

    def info(self):
        return self._client.scheduler_info()

    def cuda_cluster(self):
        return self._cluster
