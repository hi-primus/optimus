from packaging import version
from pyspark.sql import DataFrame

from optimus.helpers.columns import parse_columns
from optimus.helpers.logger import logger
from optimus.engines.spark.spark import Spark


def save(self: DataFrame):
    class Save:
        @staticmethod
        def json(path, mode="overwrite", num_partitions=1):
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
            try:
                # na.fill enforce null value keys to the json output
                self.na.fill("") \
                    .repartition(num_partitions) \
                    .write \
                    .option("encoding", encoding) \
                    .format("json") \
                    .mode(mode) \
                    .save(path)
            except IOError as e:
                logger.print(e)
                raise

        @staticmethod
        def csv(path, header="true", mode="overwrite", sep=",", num_partitions=1):
            """
            Save data frame to a CSV file.
            :param path: path where the spark will be saved.
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
                df = self
                columns = parse_columns(self, "*",
                                        filter_by_column_dtypes=["date", "array", "vector", "binary", "null"])
                df = df.cols.cast(columns, "str").repartition(num_partitions)

                # Save to csv
                df.write.options(header=header).mode(mode).csv(path, sep=sep)
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

            columns = parse_columns(self, "*", filter_by_column_dtypes=["null"])
            df = df.cols.cast(columns, "str")

            try:
                df.coalesce(num_partitions) \
                    .write \
                    .mode(mode) \
                    .parquet(path)
            except IOError as e:
                logger.print(e)
                raise

        @staticmethod
        def avro(path, mode="overwrite", num_partitions=1):
            """
            Save data frame to an avro file
            :param path: path where the spark will be saved.
            :param mode: Specifies the behavior of the save operation when data already exists.
                        "append": Append contents of this DataFrame to existing data.
                        "overwrite" (default case): Overwrite existing data.
                        "ignore": Silently ignore this operation if data already exists.
                        "error": Throw an exception if data already exists.
            :param num_partitions: the number of partitions of the DataFrame
            :return:
            """

            try:
                if version.parse(Spark.instance.spark.version) < version.parse("2.4"):
                    avro_version = "com.databricks.spark.avro"
                else:
                    avro_version = "avro"
                self.coalesce(num_partitions) \
                    .write.format(avro_version) \
                    .mode(mode) \
                    .save(path)

            except IOError as e:
                logger.print(e)
                raise


    return Save()


DataFrame.save = property(save)
