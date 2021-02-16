import dask
from dask.distributed import Client, get_client

from optimus.engines.base.create import Create
from optimus.engines.base.engine import BaseEngine
from optimus.engines.base.remote import ClientActor, RemoteDummyVariable
from optimus.engines.dask_cudf.io.load import Load
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.optimus import Engine
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

        self.engine = Engine.DASK_CUDF.value
        self.create = Create(self)
        self.load = Load(self)
        self.verbose(verbose)

        use_actor = True

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
                                     worker_gpu=1, 
                                     worker_class='dask_cuda.CUDAWorker',
                                     n_workers=n_workers, 
                                     worker_memory='15GiB', 
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

        elif session=="local":
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

        elif session:
            self.client = session

        else:
            self.client = get_client()
            use_actor = False

        if use_actor:
            self.remote = self.client.submit(ClientActor, Engine.DASK_CUDF.value, actor=True).result(10)
            self.load = RemoteDummyVariable(self, "_load")
            self.create = RemoteDummyVariable(self, "_create")

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance


    def dataframe(self, cdf, n_partitions=1, *args, **kwargs):
        import dask_cudf
        from optimus.engines.dask_cudf.dataframe import DaskCUDFDataFrame
        return DaskCUDFDataFrame(dask_cudf.from_cudf(cdf, npartitions=n_partitions, *args, **kwargs))


    def remote_run(self, callback, *args, **kwargs):
        if not getattr(self, "remote"):
            raise

        if kwargs.get("client_timeout"):
            client_timeout = kwargs.get("client_timeout")
            del kwargs["client_timeout"]

        else:
            client_timeout = 600

        result = self.remote.submit(callback, *args, **kwargs).result(client_timeout)
        if isinstance(result, dict):
            if result.get("status") == "error" and result.get("error"):
                raise Exception(result.get("error"))
            elif result.get("dummy"):
                return RemoteDummyVariable(self, result.get("dummy"))
        return result


    def remote_submit(self, callback, *args, **kwargs):
        if not getattr(self, "remote"):
            raise

        return self.remote.submit(callback, *args, **kwargs)

