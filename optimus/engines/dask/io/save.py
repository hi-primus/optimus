import os

from optimus.engines.base.io.save import BaseSave, DEFAULT_MODE
from optimus.helpers.functions import prepare_path_local, path_is_local
from optimus.helpers.logger import logger

from optimus.helpers.types import *
from optimus.engines.base.io.save import BaseSave


class Save(BaseSave):
    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def json(self, path, storage_options=None, conn=None, *args, **kwargs):

        df = self.root.data

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            os.makedirs(path, exist_ok=True)
            df.to_json(filename=path, storage_options=storage_options, *args, **kwargs)
        except (OSError, IOError) as error:
            logger.print(error)
            raise

    def csv(self, path, mode=DEFAULT_MODE, index=False, single_file=True, storage_options=None, conn=None, **kwargs):

        df = self.root.data

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            if path_is_local(path):
                prepare_path_local(path)

            df.to_csv(filename=path, mode=mode, index=index, single_file=single_file, storage_options=storage_options,
                      **kwargs)
        except IOError as error:
            logger.print(error)
            raise

    def parquet(self, path, mode=DEFAULT_MODE, num_partitions=1, engine="pyarrow", storage_options=None, conn=None,
                **kwargs):

        # This character are invalid as column names by parquet
        invalid_character = [" ", ",", ";", "{", "}", "(", ")", "\n", "\t", "="]

        def func(col_name):
            for i in invalid_character:
                col_name = col_name.replace(i, "_")
            return col_name

        df = self.root.cols.rename(func)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options
        dfd = df.data
        try:
            if engine == 'pyarrow':
                dfd.to_parquet(path, engine='pyarrow', storage_options=storage_options, **kwargs)
            elif engine == "fastparquet":
                dfd.to_parquet(path, engine='fastparquet', storage_options=storage_options, **kwargs)

        except IOError as e:
            logger.print(e)
            raise

    @staticmethod
    def avro(path):
        raise NotImplementedError('Not implemented yet')
