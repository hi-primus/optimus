from dask.distributed import Client
from optimus.dask.dask import Dask
from optimus.dask.io.load import Load
from optimus.profiler.profiler import Profiler

from optimus.bumblebee import Comm
Dask.instance = None
Profiler.instance = None
Comm.instance = None


class DaskEngine:
    def __init__(self, *args, **kwargs):
        self.engine = 'dask'
        # self.create = Create()
        self.load = Load()
        # self.read = self.spark.read

        Dask.instance = Client(n_workers=2, threads_per_worker=4, processes=False, memory_limit='2GB')

    # def create(self, data):
    #     import dask.dataframe as dd
    #     return dd.DataFrame(data)
