import GPUtil
from dask.distributed import Client
from dask_cuda import LocalCUDACluster
from dask import dataframe as dd
from optimus.engines.base.create import Create
from optimus.engines.base.engine import BaseEngine
from optimus.engines.dask_cudf.dask_cudf import DaskCUDF
from optimus.engines.dask_cudf.io.load import Load
from optimus.helpers.logger import logger
from optimus.profiler.profiler import Profiler
import dask_cudf
DaskCUDF.instance = None
Profiler.instance = None

BIG_NUMBER = 100000


class DaskCUDFEngine(BaseEngine):
    def __init__(self, session=None, n_workers=2, threads_per_worker=4, memory_limit='2GB',
                 verbose=False, comm=None, *args, **kwargs):
        """

        :param session:
        :param n_workers:
        :param threads_per_worker:
        :param memory_limit:
        :param verbose:
        :param comm:
        :param args:
        :param kwargs:
        """


        self.engine = 'dask-cudf'
        self.create = Create(dask_cudf)
        self.load = Load()
        self.verbose(verbose)

        if session is None:
            # Processes are necessary in order to use multiple GPUs with Dask

            n_gpus = len(GPUtil.getAvailable(order='first', limit=BIG_NUMBER))

            if n_workers > n_gpus:
                logger.print(f"n_workers should equal or less than the number of GPUs. n_workers is now {n_gpus}")
                n_workers = n_gpus

            cluster = LocalCUDACluster(n_workers=n_workers, threads_per_worker=threads_per_worker, processes=True,
                                       memory_limit=memory_limit)
            DaskCUDF.instance = Client(cluster, *args, **kwargs)
            # self._cluster = cluster
            # DaskCUDF.instance = DaskCUDF().create(*args, **kwargs)
            # n_workers = n_workers, threads_per_worker = threads_per_worker,
            # processes = processes, memory_limit = memory_limit
        else:
            DaskCUDF.instance = DaskCUDF().load(session)

        # Reference https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers
        self.client = DaskCUDF.instance

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance

