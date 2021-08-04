import warnings

import pandavro as pdx

from optimus.engines.base.io.save import BaseSave
from optimus.helpers.logger import logger

from optimus.helpers.types import *
from optimus.engines.base.io.save import BaseSave


class Save(BaseSave):
    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def json(self, path, mode="w", *args, **kwargs):

        df = self.root.data
        try:
            df.to_json(path, mode=mode, *args, **kwargs)

        except IOError as e:
            logger.print(e)
            raise

    def csv(self, path, mode="rb", *args, **kwargs):
        try:
            dfd = self.root.cols.cast("*", "str").data
            dfd.to_csv(path, index=False, mode=mode, *args, **kwargs)

        except IOError as error:
            logger.print(error)
            raise

    def parquet(self, path, mode="overwrite", num_partitions=1, *args, **kwargs):

        # This character are invalid as column names by parquet
        invalid_character = [" ", ",", ";", "{", "}", "(", ")", "\n", "\t", "="]

        def func(col_name):
            for i in invalid_character:
                col_name = col_name.replace(i, "_")
            return col_name

        df = self.root.cols.rename(func)

        try:
            df.data.to_parquet(path, mod=mode, numpartitions=num_partitions)
        except IOError as e:
            logger.print(e)
            raise

    def excel(self, path, **kwargs):

        try:
            # df = self.root.data
            # columns = parse_columns(self, "*",
            #                         filter_by_column_types=["date", "array", "vector", "binary", "null"])
            df = self.root.cols.cast("*", "str")

            # Dask reference
            # https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.to_csv
            df.to_pandas().to_excel(path, index=False)

        except IOError as error:
            logger.print(error)
            raise

    def orc(self, path, **kwargs):
        try:
            df = self.root.data
            df.to_orc(path, index=False, **kwargs)

        except IOError as error:
            logger.print(error)
            raise

    def avro(self, path, **kwargs):
        warnings.warn("Using CPU via pandavro to read the avro dataset")
        pdx.to_avro(path, self.root.to_pandas())
