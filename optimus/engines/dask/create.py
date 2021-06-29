import pandas as pd

from optimus.engines.base.meta import Meta
from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.helpers.check import is_pandas_dataframe
from optimus.helpers.converter import pandas_to_dask_dataframe
from optimus.infer import is_dict


class Create:
    def __init__(self, root):
        self.root = root

    def dataframe(self, dict=None, cols=None, rows=None, pdf=None, n_partitions=1, *args, **kwargs):
        """
        Helper to create dataframe:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples with the same number and types that cols
        :param pdf: a pandas dataframe
        :param n_partitions:
        :return: Dataframe
        """

        if is_dict(dict):
            pdf = pd.DataFrame(dict)
        elif pdf is None:
            pdf = pd.DataFrame(kwargs)

        ddf = pandas_to_dask_dataframe(pdf, n_partitions)
        df = DaskDataFrame(ddf)
        df.meta = Meta.set(df.meta, value={"max_cell_length": df.cols.len("*").cols.max()})
        return df
