import dask
from distributed import Client, get_client

from optimus.engines.dask.create import Create
from optimus.engines.base.engine import BaseEngine
from optimus.engines.base.remote import MAX_TIMEOUT, RemoteOptimusInterface, RemoteDummyVariable, RemoteDummyDataFrame
from optimus.engines.dask.dask import Dask
from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.engines.dask.io.load import Load
from optimus.optimus import Engine, EnginePretty
from optimus._version import __version__


class DaskEngine(BaseEngine):
    __version__ = __version__

    # Using procces or threads https://stackoverflow.com/questions/51099685/best-practices-in-setting-number-of-dask-workers
    def __init__(self, session=None, address=None, n_workers=None, threads_per_worker=None, processes=True,
                 memory_limit=None, verbose=False, coiled_token=None, *args, **kwargs):

        if n_workers is None and not coiled_token:
            import psutil
            threads_per_worker = psutil.cpu_count() * 4

        self.verbose(verbose)

        use_remote = kwargs.pop("use_remote", False)

        if coiled_token:
            import coiled
            dask.config.set({"coiled.token": coiled_token})
            try:
                coiled.Cloud()
            except Exception as error:
                raise error

            idle_timeout = kwargs.get("idle_timeout", None)

            memory_limit = memory_limit or '16 GiB'

            cluster = coiled.Cluster(
                name=kwargs.get("name"),
                worker_options={
                    **({"nthreads": threads_per_worker} if threads_per_worker else {})
                },
                n_workers=n_workers if n_workers else 4,
                worker_memory=memory_limit,
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
                memory_limit = memory_limit or '4GB'
                self.client = Client(address=address, n_workers=n_workers, threads_per_worker=threads_per_worker,
                                     processes=processes, memory_limit=memory_limit, *args, **kwargs)

        if use_remote:
            self.remote = RemoteOptimusInterface(self.client, Engine.DASK.value)

        else:
            self.remote = False

    @property
    def dask(self):
        """
        Return a Dask client object
        :return:
        """
        return self.client.dask

    @property
    def constants(self):
        from optimus.engines.base.dask.constants import Constants
        return Constants()

    @property
    def F(self):
        from optimus.engines.dask.functions import DaskFunctions
        return DaskFunctions()

    @property
    def create(self):
        if self.remote:
            return RemoteDummyVariable(self, "_create")

        else:
            return Create(self)

    @property
    def load(self):
        if self.remote:
            return RemoteDummyVariable(self, "_load")

        else:
            return Load(self)

    @property
    def engine(self):
        return Engine.DASK.value
    
    @property
    def engine_label(self):
        return EnginePretty.DASK.value

    def dataframe(self, pdf, n_partitions=1, *args, **kwargs):
        from dask import dataframe as dd
        return DaskDataFrame(dd.from_pandas(pdf, npartitions=n_partitions, *args, **kwargs))

    def remote_run(self, func, *args, **kwargs):
        if kwargs.get("client_timeout"):
            client_timeout = kwargs.get("client_timeout")
            del kwargs["client_timeout"]
        else:
            client_timeout = MAX_TIMEOUT

        return self.remote_submit(func, *args, **kwargs).result(client_timeout)

    def remote_submit(self, func, *args, **kwargs):


        if not self.remote:
            fut = self.submit(func, op=self, *args, **kwargs)
        else:
            fut = self.remote.submit(func, *args, **kwargs)

        fut.__result = fut.result
        _op = self

        def _result(self, *args, **kwargs):
            result = self.__result(*args, **kwargs)
            if isinstance(result, dict):
                if result.get("status") == "error" and result.get("error"):
                    raise Exception(result.get("error"))
                elif result.get("dummy"):
                    if result.get("dataframe"):
                        return RemoteDummyDataFrame(_op, result.get("dummy"))
                    else:
                        return RemoteDummyVariable(_op, result.get("dummy"))
            return result

        import types
        fut.result = types.MethodType(_result, fut)

        # TODO: Implement this on remote.submit

        return fut

    def submit(self, func, *args, priority=0, pure=False, **kwargs):
        from optimus.engines.base.remote import RemoteDummyAttribute
        if isinstance(func, (RemoteDummyAttribute,)):
            return func(client_submit=True, *args, **kwargs)
        return distributed.get_client().submit(func, priority=priority, pure=pure, *args, **kwargs)
