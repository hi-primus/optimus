import dask
from dask.distributed import Client

from optimus.engines.base.create import Create
from optimus.engines.base.engine import BaseEngine
from optimus.engines.dask.dask import Dask
from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.engines.dask.io.load import Load
from optimus.profiler.profiler import Profiler
from optimus.version import __version__

Dask.instance = None
Profiler.instance = None


class DaskEngine(BaseEngine):
    __version__ = __version__

    # Using procces or threads https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers
    def __init__(self, session=None, address=None, n_workers=1, threads_per_worker=None, processes=False,
                 memory_limit='4GB', verbose=False, coiled_token=None, coiled_gpu=False, *args, **kwargs):

        if n_workers is None:
            import psutil
            threads_per_worker = psutil.cpu_count() * 4

        self.verbose(verbose)

        if coiled_token:
            import coiled
            dask.config.set({"coiled.token": coiled_token})
            try:
                coiled.Cloud()
            except Exception as error:
                raise error

            cluster = coiled.Cluster(name="temp",
                                     n_workers=n_workers,
                                     # worker_memory=15,
                                     worker_options={
                                         "nthreads": threads_per_worker,
                                         "memory_limit": memory_limit,
                                     },
                                     software={
                                         coiled_gpu if "optimus/default-gpu" else "optimus/default"
                                     },
                                     )

            Dask.instance = Client(cluster)

        elif address:
            Dask.instance = Client(address=address)

        elif session is None:
            # Create a local cluster
            Dask.instance = Client(address=address, n_workers=n_workers, threads_per_worker=threads_per_worker,
                                   processes=processes,
                                   memory_limit=memory_limit, *args,
                                   **kwargs)
        else:
            Dask.instance = Dask().load(session)

        # Reference https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers
        self.client = Dask.instance

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance

    @property
    def dask(self):
        """
        Return a Spark session object
        :return:
        """
        return Dask.instance.dask

    @property
    def create(self):
        return Create(self)

    @property
    def load(self):
        return Load()

    @property
    def engine(self):
        return "dask"

    def dataframe(self, pdf, n_partitions=1, *args, **kwargs):
        from dask import dataframe as dd
        return DaskDataFrame(dd.from_pandas(pdf, npartitions=n_partitions, *args, **kwargs))
