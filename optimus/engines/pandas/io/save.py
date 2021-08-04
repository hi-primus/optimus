import os

import pandavro as pdx

from optimus.engines.base.io.save import BaseSave
from optimus.helpers.logger import logger

from optimus.helpers.types import *
from optimus.engines.base.io.save import BaseSave


class Save(BaseSave):
    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def json(self, path, orient="records", *args, **kwargs):

        df = self.root.data

        try:
            df.to_json(path, orient=orient, *args, **kwargs)

        except IOError as e:
            logger.print(e)
            raise

    def csv(self, path, mode="w", show_path=False, encoding="utf-8-sig", **kwargs):

        try:
            df = self.root.data
            # Dask reference
            # https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.to_csv
            # df.to_csv(filename=path)

            if show_path is True:
                print(os.path.abspath(path))

            df.to_csv(path, index=False, mode=mode, encoding=encoding, **kwargs)

        except IOError as error:
            logger.print(error)
            raise

    def excel(self, path, mode="w", *args, **kwargs):

        try:
            df = self.root.data
            df.to_excel(path, index=False)

        except IOError as error:
            logger.print(error)
            raise

    def parquet(self, path, *args, **kwargs):
        """
        Save data frame to a parquet file
        :param path: path where the spark will be saved.
        :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """

        # This character are invalid as column names by parquet
        invalid_character = [" ", ",", ";", "{", "}", "(", ")", "\n", "\t", "="]

        def func(col_name):
            for i in invalid_character:
                col_name = col_name.replace(i, "_")
            return col_name

        df = self.root.cols.rename(func)

        try:
            df.data.to_parquet(path, mode=mode)
        except IOError as e:
            logger.print(e)
            raise

    def avro(self, path, *args, **kwargs):
        pdx.to_avro(path, self.root.data)
