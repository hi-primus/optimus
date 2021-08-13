import pandas as pd

DataFrame = pd.DataFrame
from optimus.helpers.logger import logger
from optimus.engines.base.io.save import BaseSave


def save(self: DataFrame):
    class Save(BaseSave):
        @staticmethod
        def json(path, *args, **kwargs):
            df = self
            try:
                df.to_json(path, *args, **kwargs)

            except IOError as e:
                logger.print(e)
                raise

        @staticmethod
        def csv(path, mode="w", **kwargs):

            try:
                df = self
                # columns = parse_columns(self, "*",
                #                         filter_by_column_types=["date", "array", "vector", "binary", "null"])
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
