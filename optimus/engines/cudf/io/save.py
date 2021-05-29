import warnings

import pandavro as pdx

from optimus.helpers.logger import logger


class Save:
    def __init__(self, root):
        self.root = root

    def json(self, path, *args, **kwargs):
        """
        Save data frame in a json file
        :param path: path where the spark will be saved.
        :param mode: Specifies the behavior of the save operation when data already exists.
                "append": Append contents of this DataFrame to existing data.
                "overwrite" (default case): Overwrite existing data.
                "ignore": Silently ignore this operation if data already exists.
                "error": Throw an exception if data already exists.
        :return:
        """
        df = self.root.data
        try:
            df.to_json(path, *args, **kwargs)

        except IOError as e:
            logger.print(e)
            raise

    def csv(self, path, mode="rb", **kwargs):
        """
        Save data frame to a CSV file.
        :param path: path where the spark will be saved.
        :param mode: 'rb', 'wt', etc
        it uses the default value.
        :return: Dataframe in a CSV format in the specified path.
        """

        try:
            df = self.root
            dfd = self.root.data
            df = df.cols.cast("*", "str")
            dfd.to_csv(path, index=False, **kwargs)

        except IOError as error:
            logger.print(error)
            raise

    def parquet(self, path, mode="overwrite", num_partitions=1):
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
            df.data.to_parquet(path)
        except IOError as e:
            logger.print(e)
            raise

    def excel(self, path, **kwargs):
        """
        Save data frame to a CSV file.
        :param path: path where the spark will be saved.
        :param mode: 'rb', 'wt', etc
        it uses the default value.
        :return: Dataframe in a CSV format in the specified path.
        """

        try:
            # df = self.root.data
            # columns = parse_columns(self, "*",
            #                         filter_by_column_dtypes=["date", "array", "vector", "binary", "null"])
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
