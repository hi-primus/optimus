from dask.distributed import Client


class DaskEngine:
    def __init__(self, *args, **kwargs):
        self.engine = 'dask'
        client = Client(n_workers=1, threads_per_worker=4, processes=False, memory_limit='2GB')

    def create(self, data):
        import dask.dataframe as dd
        return dd.DataFrame(data)
