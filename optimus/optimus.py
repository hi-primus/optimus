import os
from shutil import rmtree
import logging
import sys
from functools import reduce

from optimus.spark import Spark

Spark.instance = None

import operator
from optimus.create import Create
from optimus.io.load import Load
from optimus.spark import Spark
from optimus.helpers.constants import *
from optimus.helpers.functions import random_name, is_str, is_list_of_dataframes, is_ip, is_filepath
from optimus.helpers.raiseit import RaiseIfNot
from optimus.dataframe import rows, columns, extension

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# We use this to save a reference to the Spark session at the module level


class Optimus:
    def __init__(self, master="local", app_name="optimus", path=None, file_system="local", verbose=False):
        """

        :param master:
        :param app_name:
        :param path:
        :param file_system:
        """

        RaiseIfNot.type_error(master, is_str)
        RaiseIfNot.value_error(master, ["master", "local"])

        RaiseIfNot.type_error(app_name, is_str)

        RaiseIfNot.type_error(path, is_str)
        RaiseIfNot.value_error(path, is_filepath)

        RaiseIfNot.value_error(master, ["local", "hadoop"])

        if verbose:
            level = logging.INFO
            logging.basicConfig(format="%(message)s", level=level)
        else:
            logging.propagate = False
            logging.disable(logging.NOTSET)

        if path is None:
            path = os.getcwd()

        # Initialize Spark
        Spark.instance = Spark(master, app_name)
        self.set_check_point_folder(path, file_system)

        self.create = Create()
        self.load = Load()

        self.spark = self.get_ss()
        self.read = self.spark.read

    @staticmethod
    def get_ss():
        return Spark.instance.get_ss()

    @staticmethod
    def get_sc():
        return Spark.instance.get_sc()

    @staticmethod
    def concat(dfs, like="columns"):
        """
        Concat multiple dataframes as columns or rows way
        :param dfs:
        :param like: The way dataframes is going to be concat. like columns or rows
        :return:
        """
        RaiseIfNot.type_error(dfs, is_list_of_dataframes)

        RaiseIfNot.value_error(like, ["columns", "rows"])

        # Add increasing Ids, and they should be the same.
        if like == "columns":
            temp_dfs = []
            col_temp_name = "id_" + random_name()
            for df in dfs:
                temp_dfs.append(df.withColumn(col_temp_name, F.monotonically_increasing_id()))

            def _append_df(df1, df2):
                return df2.join(df1, col_temp_name, "outer").drop(col_temp_name)

            df_result = reduce(_append_df, temp_dfs)
        elif like == "rows":
            df_result = reduce(DataFrame.union, dfs)

        return df_result

        # Alias

    def set_check_point_folder(self, path, file_system):
        """
        Function that receives a workspace path where a folder is created.
        This folder will store temporal
        dataframes when user writes the DataFrameTransformer.checkPoint().

        This function needs the sc parameter, which is the spark context in order to
        tell spark where is going to save the temporal files.

        It is recommended that users deleted this folder after all transformations are completed
        and the final dataframe have been saved. This can be done with deletedCheckPointFolder function.

        :param path: Location of the dataset (string).
        :param file_system: Describes if file system is local or hadoop file system.

        """
        RaiseIfNot.type_error(file_system, is_filepath)

        print_check_point_config(file_system)

        if file_system == "hadoop":
            folder_path = path + "/" + "checkPointFolder"
            self.delete_check_point_folder(path=path, file_system=file_system)

            # Creating file:
            logging.info("Creating the hadoop folder...")
            command = "hadoop fs -mkdir " + folder_path
            logging.info("$" + command)
            os.system(command)
            logging.info("Hadoop folder created. \n")

            logging.info("Setting created folder as checkpoint folder...")
            Spark.instance.get_sc().setCheckpointDir(folder_path)
        elif file_system == "local":
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            logging.info("Deleting previous folder if exists...")
            if os.path.isdir(folder_path):
                # Deletes folder if exits:
                rmtree(folder_path)

            logging.info("Creating the checkpoint directory...")
            # Creates new folder:
            os.mkdir(folder_path)

            Spark.instance.get_sc().setCheckpointDir(dirName="file:///" + folder_path)
        else:
            raise ValueError("file_system must be 'local' or 'hadoop', received {0}".format(file_system))

    @staticmethod
    def delete_check_point_folder(path, file_system):
        """
        Function that deletes the temporal folder where temp files were stored.
        The path required is the same provided by user in setCheckPointFolder().

        :param path:
        :param file_system: Describes if file system is local or hadoop file system.
        :return:
        """
        RaiseIfNot.type_error(file_system, is_filepath)

        RaiseIfNot.value_error(file_system, ["hadoop", "local"])

        if file_system == "hadoop":
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            logging.info("Deleting checkpoint folder...")
            command = "hadoop fs -rm -r " + folder_path
            os.system(command)
            logging.info("$" + command)
            logging.info("Folder deleted.")
        elif file_system == "local":
            logging.info("Deleting checkpoint folder...")
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            if os.path.isdir(folder_path):
                # Deletes folder if exits:
                rmtree(folder_path)
                # Creates new folder:
                logging.info("Folder deleted.")
            else:
                logging.info("Folder deleted.")
                pass
