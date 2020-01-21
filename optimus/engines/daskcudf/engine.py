from dask.distributed import Client
from dask_cuda import LocalCUDACluster

from optimus.bumblebee import Comm
from optimus.engines.dask.dask import Dask
from optimus.engines.dask.io.load import Load
from optimus.profiler.profiler import Profiler

Dask.instance = None
Profiler.instance = None
Comm.instance = None


class DaskCUDFEngine:
    def __init__(self, *args, **kwargs):
        self.engine = 'dask_cudf'
        # self.create = Create()
        self.load = Load()
        # self.read = self.spark.read

        cluster = LocalCUDACluster()
        client = Client(cluster)

        Dask.instance = client

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance
