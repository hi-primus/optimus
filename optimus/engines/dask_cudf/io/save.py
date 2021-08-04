from optimus.engines.base.io.save import BaseSave, DEFAULT_MODE
from optimus.helpers.logger import logger

from optimus.helpers.types import *
from optimus.engines.base.io.save import BaseSave


class Save(BaseSave):
    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def json(self, path, *args, **kwargs):

        df = self.root.data
        try:
            df.to_json(filename=path, *args, **kwargs)

        except IOError as e:
            logger.print(e)
            raise

    def csv(self, path, mode=DEFAULT_MODE, *args, **kwargs):

        try:
            df = self.root.data
            # columns = parse_columns(self, "*",
            #                         filter_by_column_types=["date", "array", "vector", "binary", "null"])
            # df = df.cols.cast(columns, "str").repartition(num_partitions)

            # Dask reference
            # https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.to_csv
            # df.to_csv(filename=path)
            df.to_csv(filename=path, mode=mode, *args, **kwargs)

        except IOError as error:
            logger.print(error)
            raise

    def parquet(self, path, mode=DEFAULT_MODE, num_partitions=1, compression="snappy", *args, **kwargs):

        # This character are invalid as column names by parquet
        invalid_character = [" ", ",", ";", "{", "}", "(", ")", "\n", "\t", "="]

        def func(col_name):
            for i in invalid_character:
                col_name = col_name.replace(i, "_")
            return col_name

        df = self.root.cols.rename(func)

        try:
            df.to_parquet(path, compression=compression)
            print(path)
        except IOError as e:
            logger.print(e)
            raise

    def avro(self, path, *args, **kwargs):
        raise NotImplementedError('Not implemented yet')
