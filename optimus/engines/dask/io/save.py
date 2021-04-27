import os

from optimus.helpers.logger import logger
from optimus.helpers.functions import prepare_path_local, path_is_local


class Save:
    def __init__(self, root):
        self.root = root

    def json(self, path, storage_options=None, conn=None, *args, **kwargs):
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
        #     print("Creation of the directory %s failed" % path)
        # else:
        #     print("Successfully created the directory %s" % path)

    def csv(self, path, mode="wt", index=False, single_file=True, storage_options=None, conn=None, **kwargs):
        """
        Save data frame to a CSV file.
        :param mode: 'rb', 'wt', etc
        :param single_file:
        :param index:
        :param path: path where the spark will be saved.
        it uses the default value.
        :return: Dataframe in a CSV format in the specified path.
        """

        df = self.root.data

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            if path_is_local(path):
                prepare_path_local(path)
            
            df.to_csv(filename=path, mode=mode, index=index, single_file=single_file, storage_options=storage_options, **kwargs)
        except IOError as error:
            logger.print(error)
            raise

    def parquet(self, path, mode="overwrite", num_partitions=1, engine="pyarrow", storage_options=None, conn=None, **kwargs):
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

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options
        dfd = df.data
        try:
            if engine == 'pyarrow':
                dfd.to_parquet(path,  engine='pyarrow', storage_options=storage_options, **kwargs)
            elif engine == "fastparquet":
                dfd.to_parquet(path, engine='fastparquet', storage_options=storage_options, **kwargs)

        except IOError as e:
            logger.print(e)
            raise

    @staticmethod
    def avro(path):
        raise NotImplementedError('Not implemented yet')
