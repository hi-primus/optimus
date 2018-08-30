import logging

from pyspark.sql import DataFrame

from optimus.helpers.decorators import *


def save(self):
    @add_attr(save)
    def json(path, mode="overwrite", num_partitions=1):
        """
        Save data frame in a json file
        :param path: path where the dataframe will be saved.
        :param mode: Specifies the behavior of the save operation when data already exists.
                "append": Append contents of this DataFrame to existing data.
                "overwrite" (default case): Overwrite existing data.
                "ignore": Silently ignore this operation if data already exists.
                "error": Throw an exception if data already exists.
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """
        try:
            # na.fill enforce null value keys to the json output
            self.na.fill("") \
                .repartition(num_partitions) \
                .write \
                .format("json") \
                .mode(mode) \
                .save(path)
        except IOError as e:
            logging.error(e)
            raise

    @add_attr(save)
    def csv(path, header="true", mode="overwrite", sep=",", num_partitions=1):
        """
        Save data frame to a CSV file.
        :param path: path where the dataframe will be saved.
        :param header: True or False to include header
        :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
        :param sep: sets the single character as a separator for each field and value. If None is set,
        it uses the default value.
        :param num_partitions: the number of partitions of the DataFrame
        :return: Dataframe in a CSV format in the specified path.
        """

        try:
            self.repartition(num_partitions).write.options(header=header).mode(mode).csv(path, sep=sep)
        except IOError as error:
            logging.error(error)
            raise

    @add_attr(save)
    def parquet(path, mode="overwrite", num_partitions=1):
        """
        Save data frame to a parquet file
        :param path: path where the dataframe will be saved.
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
            df.coalesce(num_partitions) \
                .write \
                .mode(mode) \
                .parquet(path)
        except IOError as e:
            logging.error(e)
            raise

    @add_attr(save)
    def avro(path, mode="overwrite", num_partitions=1):
        """
        Save data frame to an avro file
        :param path: path where the dataframe will be saved.
        :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
        :param num_partitions: the number of partitions of the DataFrame
        :return:
        """

        try:
            self.coalesce(num_partitions) \
                .write.format("com.databricks.spark.avro") \
                .mode(mode) \
                .save(path)
        except IOError as e:
            logging.error(e)
            raise

    return save


DataFrame.save = property(save)
