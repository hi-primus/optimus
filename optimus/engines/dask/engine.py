from dask import dataframe as dd
from dask.distributed import Client
from optimus.engines.base.create import Create
from optimus.bumblebee import Comm
from optimus.engines.base.engine import BaseEngine
from optimus.engines.dask.dask import Dask
from optimus.engines.dask.io.load import Load
from optimus.profiler.profiler import Profiler
from optimus.version import __version__

Dask.instance = None
Profiler.instance = None
Comm.instance = None


class DaskEngine(BaseEngine):
    __version__ = __version__

    # Using procces or threads https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers
    def __init__(self, session=None, n_workers=1, threads_per_worker=None, processes=False, memory_limit='4GB',
                 verbose=False, comm=None, *args, **kwargs):

        if comm is True:
            Comm.instance = Comm()
        else:
            Comm.instance = comm

        self.engine = 'dask'

        if n_workers is None:
            import psutil
            threads_per_worker = psutil.cpu_count() * 4

        self.create = Create(dd)
        self.load = Load()
        self.verbose(verbose)

        if session is None:
            # print("PROCESS", processes)
            Dask.instance = Client(n_workers=n_workers, threads_per_worker=threads_per_worker, processes=processes,
                                   memory_limit=memory_limit, *args,
                                   **kwargs)
            # a = Dask()
            # b = a.create(n_workers=n_workers, threads_per_worker=threads_per_worker,
            #              processes=processes, memory_limit=memory_limit, *args, **kwargs)
            # Dask.instance = b
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

