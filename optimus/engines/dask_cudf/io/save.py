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
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """
        df = self.root.data
        try:
            df.to_json(filename=path, *args, **kwargs)

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
            df = self.root.data
            # columns = parse_columns(self, "*",
            #                         filter_by_column_dtypes=["date", "array", "vector", "binary", "null"])
            # df = df.cols.cast(columns, "str").repartition(num_partitions)

            # Dask reference
            # https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.to_csv
            # df.to_csv(filename=path)
            df.to_csv(filename=path, mode=mode, **kwargs)

        except IOError as error:
            logger.print(error)
            raise

    def parquet(self, path, compression="snappy", mode="overwrite", num_partitions=1):
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
            df.to_parquet(path, compression='snappy')
            print(path)
        except IOError as e:
            logger.print(e)
            raise

    @staticmethod
    def avro(path):
        raise NotImplementedError('Not implemented yet')
