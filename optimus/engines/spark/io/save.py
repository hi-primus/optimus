from packaging import version

from optimus.engines.base.io.save import BaseSave, DEFAULT_MODE
from optimus.engines.spark.spark import Spark
from optimus.helpers.columns import parse_columns
from optimus.helpers.functions import path_is_local, prepare_path_local
from optimus.helpers.logger import logger

from optimus.helpers.types import *
from optimus.engines.base.io.save import BaseSave


class Save(BaseSave):

    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def json(self, path, mode="overwrite", encoding="UTF-8", num_partitions=1):

        df = self.root.data
        try:
            # na.fill enforce null value keys to the json output
            df.na.fill("") \
                .repartition(num_partitions) \
                .write \
                .option("encoding", encoding) \
                .format("json") \
                .mode(mode) \
                .save(path)
        except IOError as e:
            logger.print(e)
            raise

    def csv(self, path, header="true", mode="overwrite", single_file=True, storage_options=None, conn=None, sep=",",
            num_partitions=1):

        try:
            df = self.root

            if conn is not None:
                path = conn.path(path)
                storage_options = conn.storage_options

            try:
                if path_is_local(path):
                    prepare_path_local(path)
            except IOError as error:
                logger.print(error)
                raise

            columns = parse_columns(df, "*",
                                    filter_by_column_types=["date", "array", "vector", "binary", "null"])
            df = df.cols.cast(columns, "str").repartition(num_partitions)

            # Save to csv
            if single_file is True:
                print(path)
                # df.data.repartition(1).write.csv(path)
                df.data.toPandas().to_csv(path, header=True)
                # df.data.repartition(1).write.format('com.databricks.spark.csv').save(path,
                #                                                                 header='true')
            else:
                df.data.write.options(header=header).mode(mode).csv(path, sep=sep)

            # val conf    = sc.hadoopConfiguration
            # val src     = new Path(tmpFolder)
            # val fs      = src.getFileSystem(conf)
            # val oneFile = fs.listStatus(src).map(x => x.getPath.toString()).find(x => x.endsWith(format))
            # val srcFile = new Path(oneFile.getOrElse(""))
            # val dest    = new Path(filename)
            # fs.rename(srcFile, dest)
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

        df = self.root

        def func(col_name):
            for i in invalid_character:
                col_name = col_name.replace(i, "_")
            return col_name

        df = df.cols.rename(func)

        columns = parse_columns(df, "*", filter_by_column_types=["null"])
        df = df.cols.cast(columns, "str")

        try:
            df.coalesce(num_partitions) \
                .write \
                .mode(mode) \
                .parquet(path)
        except IOError as e:
            logger.print(e)
            raise

    def avro(self, path, mode="overwrite", num_partitions=1):

        df = self.root
        try:
            if version.parse(Spark.instance.spark.version) < version.parse("2.4"):
                avro_version = "com.databricks.spark.avro"
            else:
                avro_version = "avro"
            df.coalesce(num_partitions) \
                .write.format(avro_version) \
                .mode(mode) \
                .save(path)

        except IOError as e:
            logger.print(e)
            raise
