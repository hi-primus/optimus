import io

import pandas as pd
import requests

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.io.reader import Reader
from optimus.engines.pyodide.dataframe import PyodideDataFrame
from optimus.infer import is_url


class Load(BaseLoad):

    def hdf5(self, path, columns=None, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        pass

    def orc(self, path, columns, storage_options=None, conn=None, n_partitions=None, *args,
            **kwargs) -> 'DataFrameType':
        pass

    @staticmethod
    def df(*args, **kwargs):
        return PyodideDataFrame(*args, **kwargs)

    @staticmethod
    def _csv(filepath_or_buffer, n_rows=1000, callback=None, *args, **kwargs):
        import pyodide
        kwargs.pop("n_partitions", None)
        if is_url(filepath_or_buffer):
            df = pd.read_csv(pyodide.http.open_url(filepath_or_buffer), *args, **kwargs)
        else:
            buffer = filepath_or_buffer
            df = pd.read_csv(buffer, *args, **kwargs)

        return df

    @staticmethod
    def _json(filepath_or_buffer, *args, **kwargs):
        pass

    @staticmethod
    def _avro(filepath_or_buffer, nrows=None, *args, **kwargs):
        pass

    @staticmethod
    def _parquet(filepath_or_buffer, nrows=None, engine="pyarrow", *args, **kwargs):
        pass

    @staticmethod
    def _xml(filepath_or_buffer, nrows=None, *args, **kwargs):
        pass

    @staticmethod
    def _excel(filepath_or_buffer, nrows=None, storage_options=None, *args, **kwargs):
        pass
