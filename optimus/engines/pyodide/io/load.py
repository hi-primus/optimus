import io

import pandas as pd
import requests

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.pyodide.dataframe import PyodideDataFrame


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
    def _csv(filepath_or_buffer, *args, **kwargs):
        kwargs.pop("n_partitions", None)

        try:
            response = requests.get(filepath_or_buffer)
            s = response.content
            df = pd.read_csv(io.StringIO(s.decode('utf-8')), *args, **kwargs)

            return df
        except requests.exceptions.HTTPError as err:
            print(err)

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
