import dask

from dask.distributed import Client

from optimus.engines.base.create import Create
from optimus.engines.base.engine import BaseEngine
from optimus.engines.dask_cudf.io.load import Load
from optimus.helpers.logger import logger
from optimus.profiler.profiler import Profiler

# import dask_cudf
Profiler.instance = None

BIG_NUMBER = 100000


class DaskCUDFEngine(BaseEngine):
    def __init__(self, session=None, address=None, n_workers=1, threads_per_worker=None, processes=False,
                 memory_limit='4GB', verbose=False, coiled_token=None, *args, **kwargs):

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
        self.create = Create(self)
        self.load = Load()
        self.verbose(verbose)

        if coiled_token:
            import coiled
            print(coiled_token)
            dask.config.set({"coiled.token": coiled_token})
            try:
                coiled.Cloud()
            except Exception as error:
                raise error
            coiled.create_cluster_configuration(name="default-gpu", software="optimus/default-gpu")
            cluster = coiled.Cluster(name="temp",
                                     n_workers=n_workers,
                                     # worker_memory=15,
                                     worker_options={
                                         "nthreads": threads_per_worker,
                                         "memory_limit": memory_limit,
                                     },
                                     # software="optimus/default-gpu",
                                     configuration="default-gpu",
                                     worker_gpu=1
                                     )

            self.client = Client(cluster)

        elif address:
            self.client = Client(address=address)

        elif session is None:
            from dask_cuda import LocalCUDACluster
            from GPUtil import GPUtil
            n_gpus = len(GPUtil.getAvailable(order='first', limit=BIG_NUMBER))

            if n_workers > n_gpus:
                logger.print(f"n_workers should be equal or less than the number of GPUs. n_workers is now {n_gpus}")
                n_workers = n_gpus
                # n_gpus = 1
            cluster = LocalCUDACluster(n_workers=n_workers, threads_per_worker=threads_per_worker, processes=True,
                                       memory_limit=memory_limit)
            self.client = Client(cluster, *args, **kwargs)

        else:
            self.client = session

        # Reference https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance

    def dataframe(self, cdf, n_partitions=1, *args, **kwargs):
        import dask_cudf
        from optimus.engines.dask_cudf.dataframe import DaskCUDFDataFrame
        return DaskCUDFDataFrame(dask_cudf.from_cudf(cdf, npartitions=n_partitions, *args, **kwargs))

# import GPUtil
# from dask.distributed import Client
# from dask_cuda import LocalCUDACluster
# from dask import dataframe as dd
# from optimus.engines.base.create import Create
# from optimus.engines.base.engine import BaseEngine
# from optimus.engines.dask_cudf.dask_cudf import DaskCUDF
# from optimus.engines.dask_cudf.io.load import Load
# from optimus.helpers.logger import logger
# from optimus.profiler.profiler import Profiler
# import dask_cudf
# self.client = None
# Profiler.instance = None
#
# BIG_NUMBER = 100000
#
#
# class DaskCUDFEngine(BaseEngine):
#     def __init__(self, session=None, n_workers=2, threads_per_worker=4, memory_limit='2GB',
#                  verbose=False, comm=None, *args, **kwargs):
#         """
#
#         :param session:
#         :param n_workers:
#         :param threads_per_worker:
#         :param memory_limit:
#         :param verbose:
#         :param comm:
#         :param args:
#         :param kwargs:
#         """
#
#
#         self.engine = 'dask-cudf'
#         self.create = Create(dask_cudf)
#         self.load = Load()
#         self.verbose(verbose)
#
#         if session is None:
#             # Processes are necessary in order to use multiple GPUs with Dask
#
#             # n_gpus = len(GPUtil.getAvailable(order='first', limit=BIG_NUMBER))
#             #
#             # if n_workers > n_gpus:
#             #     logger.print(f"n_workers should equal or less than the number of GPUs. n_workers is now {n_gpus}")
#             #     n_workers = n_gpus
#             n_gpus = 1
#             cluster = LocalCUDACluster(n_workers=n_workers, threads_per_worker=threads_per_worker, processes=True,
#                                        memory_limit=memory_limit)
#             self.client = Client(cluster, *args, **kwargs)
#             # self._cluster = cluster
#             # self.client = DaskCUDF().create(*args, **kwargs)
#             # n_workers = n_workers, threads_per_worker = threads_per_worker,
#             # processes = processes, memory_limit = memory_limit
#         else:
#             self.client = DaskCUDF().load(session)
#
#         # Reference https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers
#         self.client = self.client
#
#         Profiler.instance = Profiler()
#         self.profiler = Profiler.instance
