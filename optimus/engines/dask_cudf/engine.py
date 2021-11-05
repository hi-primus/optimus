import dask
from distributed import Client, get_client

from optimus.engines.base.engine import BaseEngine
from optimus.engines.base.remote import MAX_TIMEOUT, RemoteOptimusInterface, RemoteDummyVariable, RemoteDummyDataFrame
from optimus.engines.dask_cudf.io.load import Load
from optimus.helpers.logger import logger
from optimus.optimus import Engine, EnginePretty

BIG_NUMBER = 100000


class DaskCUDFEngine(BaseEngine):
    def __init__(self, session=None, address=None, n_workers=1, threads_per_worker=8, processes=False,
                 memory_limit=None, verbose=False, coiled_token=None, *args, **kwargs):

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

        self.engine = Engine.DASK_CUDF.value

        self.verbose(verbose)

        use_remote = kwargs.get("use_remote", coiled_token is not None)

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
                worker_gpu=1,
                worker_class='dask_cuda.CUDAWorker',
                n_workers=n_workers if n_workers else 4,
                worker_memory=memory_limit,
                backend_options={
                    "region": kwargs.get("backend_region", "us-east-1")
                },
                scheduler_options={
                    **({"idle_timeout": idle_timeout} if idle_timeout else {})
                },
                software="optimus/gpu"
            )

            self.cluster_name = cluster.name
            self.client = Client(cluster)

        elif address:
            self.client = Client(address=address)

        elif session and session != "local":
            self.client = session

        else:
            try:
                self.client = get_client()
            except ValueError:
                from dask_cuda import LocalCUDACluster
                from GPUtil import GPUtil
                available_gpus = GPUtil.getAvailable(order='first', limit=BIG_NUMBER)
                n_gpus = len(available_gpus)

                if n_workers > n_gpus:
                    logger.print(
                        f"n_workers should be equal or less than the number of GPUs. n_workers is now {n_gpus}")
                    n_workers = n_gpus
                    # n_gpus = 1

                memoryTotal = GPUtil.getGPUs()[available_gpus[0]].memoryTotal

                cluster = LocalCUDACluster(rmm_pool_size=memoryTotal,
                                           device_memory_limit=memoryTotal * 0.8
                                           # Spill to RAM when 80% memory is full
                                           )
                memory_limit = memory_limit or '4GB'
                self.client = Client(cluster, memory_limit=memory_limit, *args, **kwargs)

        if use_remote:
            self.remote = RemoteOptimusInterface(self.client, Engine.DASK_CUDF.value)

        else:
            self.remote = False

    @property
    def constants(self):
        from optimus.engines.base.dask.constants import Constants
        return Constants()

    @property
    def F(self):
        from optimus.engines.dask_cudf.functions import DaskCUDFFunctions
        return DaskCUDFFunctions()

    @property
    def create(self):
        if self.remote:
            return RemoteDummyVariable(self, "_create")

        else:
            from optimus.engines.dask_cudf.create import Create
            return Create(self)

    @property
    def load(self):
        if self.remote:
            return RemoteDummyVariable(self, "_load")

        else:
            return Load(self)

    def dataframe(self, cdf, n_partitions=1, *args, **kwargs):
        import dask_cudf
        from optimus.engines.dask_cudf.dataframe import DaskCUDFDataFrame
        return DaskCUDFDataFrame(dask_cudf.from_cudf(cdf, npartitions=n_partitions, *args, **kwargs))

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

        return fut

    def submit(self, func, *args, priority=0, pure=False, **kwargs):
        from optimus.engines.base.remote import RemoteDummyAttribute
        if isinstance(func, (RemoteDummyAttribute,)):
            return func(client_submit=True, *args, **kwargs)
        return distributed.get_client().submit(func, priority=priority, pure=pure, *args, **kwargs)

    @property
    def engine_label(self):
        return EnginePretty.DASK_CUDF.value