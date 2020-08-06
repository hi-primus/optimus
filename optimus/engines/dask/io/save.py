# from pyspark.sql import DataFrame
import os
from pathlib import Path

from dask.dataframe.core import DataFrame

from optimus.helpers.logger import logger


def save(self: DataFrame):
    class Save:
        @staticmethod
        def json(path, *args, **kwargs):
            """
            Save data frame in a json file
            :param path: path where the spark will be saved.
            :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
            :param num_partitions: the number of partitions of the DataFrame
            :return:
            """
            df = self
            try:
                os.makedirs(path, exist_ok=True)
                df.to_json(filename=path, *args, **kwargs)
            except (OSError, IOError) as error:
                logger.print(error)
                raise
            #     print("Creation of the directory %s failed" % path)
            # else:
            #     print("Successfully created the directory %s" % path)

        @staticmethod
        def csv(path, mode="wt", return_path=False, **kwargs):
            """
            Save data frame to a CSV file.
            :param path: path where the spark will be saved.
            :param mode: 'rb', 'wt', etc
            it uses the default value.
            :return: Dataframe in a CSV format in the specified path.
            """

            df = self
            try:
                os.makedirs(path, exist_ok=True)
                df.to_csv(filename=path, mode=mode, index=False, **kwargs)
            except IOError as error:
                logger.print(error)
                raise

            if return_path is True:
                return Path(path).absolute().as_uri()

        @staticmethod
        def parquet(path, mode="overwrite", num_partitions=1, engine="pyarrow"):
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

            df = self.cols.rename(func)

            try:
                if engine == 'pyarrow':
                    df.to_parquet(path, num_partitions=num_partitions, engine='pyarrow')
                elif engine == "fastparquet":
                    df.to_parquet(path, engine='fastparquet')

            except IOError as e:
                logger.print(e)
                raise

        @staticmethod
        def avro(path):
            raise NotImplementedError('Not implemented yet')

        @staticmethod
        def rabbit_mq(host, exchange_name=None, queue_name=None, routing_key=None, parallelism=None):
            raise NotImplementedError('Not implemented yet')

    return Save()


DataFrame.save = property(save)
