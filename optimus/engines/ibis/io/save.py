import pandas as pd

DataFrame = pd.DataFrame
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
                df.to_json(path, *args, **kwargs)

            except IOError as e:
                logger.print(e)
                raise

        @staticmethod
        def csv(path, mode="w", **kwargs):
            """
            Save data frame to a CSV file.
            :param path: path where the spark will be saved.
            :param mode: 'rb', 'wt', etc
            it uses the default value.
            :return: Dataframe in a CSV format in the specified path.
            """

            try:
                df = self
                # columns = parse_columns(self, "*",
                #                         filter_by_column_dtypes=["date", "array", "vector", "binary", "null"])
                # df = df.cols.cast(columns, "str").repartition(num_partitions)

                # Dask reference
                # https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.to_csv
                # df.to_csv(filename=path)
                df.to_csv(path, index=False, mode=mode)

            except IOError as error:
                logger.print(error)
                raise

        @staticmethod
        def parquet(path, mode="overwrite", num_partitions=1):
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
                df.to_parquet(path, num_partitions=num_partitions)
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
