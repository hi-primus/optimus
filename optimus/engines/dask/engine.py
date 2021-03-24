import dask
from dask.distributed import Client, get_client

from optimus.engines.dask.create import Create
from optimus.engines.base.engine import BaseEngine
from optimus.engines.dask.dask import Dask
from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.engines.dask.io.load import Load
from optimus.optimus import Engine
from optimus.version import __version__


class DaskEngine(BaseEngine):
    __version__ = __version__

    # Using procces or threads https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers
    def __init__(self, session=None, address=None, n_workers=1, threads_per_worker=None, processes=False,
                 memory_limit='4GB', verbose=False, coiled_token=None, *args, **kwargs):

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

            idle_timeout = kwargs.get("idle_timeout", None)

            cluster = coiled.Cluster(
                name=kwargs.get("name"),
                worker_options={
                    **({"nthreads": threads_per_worker} if threads_per_worker else {}),
                    **({"memory_limit": memory_limit} if memory_limit else {})
                },
                n_workers=n_workers,
                worker_memory='15GiB',
                scheduler_options={
                    **({"idle_timeout": idle_timeout} if idle_timeout else {})
                },
                software="optimus/default"
            )

            self.cluster_name = cluster.name
            self.client = Client(cluster)


        elif address:
            self.client = Client(address=address)

        elif session and session != "local":
            self.client = Dask().load(session)

        else:
            try:
                self.client = get_client()
            except ValueError:
                self.client = Client(address=address, n_workers=n_workers, threads_per_worker=threads_per_worker,
                                     processes=processes, memory_limit=memory_limit, *args, **kwargs)

    @property
    def dask(self):
        """
        Return a Spark session object
        :return:
        """
        return self.client.dask

    @property
    def create(self):
        return Create(self)

    @property
    def load(self):
        return Load(self)

    @property
    def engine(self):
        return Engine.DASK.value

    def dataframe(self, pdf, n_partitions=1, *args, **kwargs):
        from dask import dataframe as dd
        return DaskDataFrame(dd.from_pandas(pdf, npartitions=n_partitions, *args, **kwargs))

    def remote_run(self, callback, *args, **kwargs):
        if kwargs.get("client_timeout"):
            del kwargs["client_timeout"]
        
        self.submit(callback, op=self, *args, **kwargs).result()

    def remote_submit(self, callback, *args, **kwargs):
        return self.submit(callback, op=self, *args, **kwargs)

    def submit(self, func,*args, **kwargs):
        return dask.distributed.get_client().submit(func, *args, **kwargs)