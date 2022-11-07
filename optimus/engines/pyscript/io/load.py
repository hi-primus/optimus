import asyncio

import pandas as pd

from js import fetch

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.pandas.dataframe import PandasDataFrame


async def file_from_url(url, path=None):
    original_file_name = url.split("/")[-1]
    path = path if path else f"/{original_file_name}"
    file = await fetch(url)
    js_buffer = await file.arrayBuffer()
    return await file_from_js(js_buffer, path)


async def file_from_js(js_buffer, path):
    py_buffer = js_buffer.to_py()  # this is a memory view
    stream = py_buffer.tobytes()  # now we have a bytes object
    with open(path, "wb") as fh:
        fh.write(stream)
    return path


async def load_csv(url_or_buffer, args, kwargs):
    if isinstance(url_or_buffer, str):
        file_name = await file_from_url(url_or_buffer)
    else:
        file_name = await file_from_js(url_or_buffer, "local_file")  # TODO get name from file
    # table_name = file_name.split("/")[-1].split(".")[0]
    # file_name = url_or_buffer
    print("file_name", file_name)
    df = pd.read_csv(file_name, *args, **kwargs)
    return df


class Load(BaseLoad):

    def hdf5(self, path, columns=None, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        pass

    def orc(self, path, columns, storage_options=None, conn=None, n_partitions=None, *args,
            **kwargs) -> 'DataFrameType':
        pass

    @staticmethod
    def df(*args, **kwargs):
        return PandasDataFrame(*args, **kwargs)

    @staticmethod
    def _csv(filepath_or_buffer, *args, **kwargs):
        kwargs.pop("n_partitions", None)

        try:
            df = await load_csv(filepath_or_buffer, args, kwargs)
            return df
        except Exception as err:
            raise err

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
