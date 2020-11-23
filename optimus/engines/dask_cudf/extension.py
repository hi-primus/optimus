import dask

from optimus.engines.base.dask.extension import Ext as DaskExtension


class Ext(DaskExtension):

    def __init__(self, root, data):
        super().__init__(root, data)

    def to_pandas(self):
        return self.data.compute().to_pandas()

    @staticmethod
    def delayed(func):
        def wrapper(*args, **kwargs):
            return dask.delayed(func)(*args, **kwargs)

        return wrapper

    def to_dict(self, orient="records", limit=None):
        """
        Create a dict
        :param orient:
        :param limit:
        :return:
        """
        series = self.root
        return series.compute().to_pandas().to_dict(orient)

