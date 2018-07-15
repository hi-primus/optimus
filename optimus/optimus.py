from optimus.dataframe.extension import Create
from optimus.io.save import Save
from optimus.io.load import Load
from optimus.spark import Spark
from optimus.helpers.constants import *

from optimus.profiler.profiler import Profiler

from optimus.df_outliers import *

from pyspark.sql import DataFrame

import os
from shutil import rmtree
from functools import reduce

# Bound columns and row operations to the DataFrame Spark Class
from optimus.dataframe import columns, rows


class Optimus:
    def __init__(self, master="local", app_name="optimus", path=None, file_system="local"):
        """

        :param master:
        :param app_name:
        :param path:
        :param file_system:
        """

        if path is None:
            path = os.getcwd()

        self.create = Create()
        self.load = Load()
        self.save = Save()

        # self.profiler = Profiler()

        Spark.instance = Spark(master, app_name)
        self.set_check_point_folder(path, file_system)
        print(SUCCESS)

    def profiler(self, df):
        return Profiler(df)

    @staticmethod
    def get_ss():
        return Spark.instance.get_ss()

    @staticmethod
    def get_sc():
        return Spark.instance.get_sc()

    def concat(self, *dfs):
        return reduce(DataFrame.union, dfs)

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
        assert (isinstance(file_system, str)), \
            "Error: file_system argument must be a string."

        assert (file_system == "hadoop") or (file_system == "local"), \
            "Error, file_system argument only can be 'local' or 'hadoop'"

        print_check_point_config(file_system)

        if file_system == "hadoop":
            folder_path = path + "/" + "checkPointFolder"
            self.delete_check_point_folder(path=path, file_system=file_system)

            # Creating file:
            print("Creating the hadoop folder...")
            command = "hadoop fs -mkdir " + folder_path
            print("$" + command)
            os.system(command)
            print("Hadoop folder created. \n")

            print("Setting created folder as checkpoint folder...")
            Spark.instance.get_sc().setCheckpointDir(folder_path)
        else:
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            print("Deleting previous folder if exists...")
            if os.path.isdir(folder_path):
                # Deletes folder if exits:
                rmtree(folder_path)

            print("Creating the checkpoint directory...")
            # Creates new folder:
            os.mkdir(folder_path)

            Spark.instance.get_sc().setCheckpointDir(dirName="file:///" + folder_path)

    @staticmethod
    def delete_check_point_folder(path, file_system):
        """
        Function that deletes the temporal folder where temp files were stored.
        The path required is the same provided by user in setCheckPointFolder().

        :param path:
        :param file_system: Describes if file system is local or hadoop file system.
        :return:
        """

        assert (isinstance(file_system, str)), "Error: file_system argument must be a string."

        assert (file_system == "hadoop") or (file_system == "local"), \
            "Error, file_system argument only can be 'local' or 'hadoop'"

        if file_system == "hadoop":
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            print("Deleting checkpoint folder...")
            command = "hadoop fs -rm -r " + folder_path
            os.system(command)
            print("$" + command)
            print("Folder deleted. \n")
        else:
            print("Deleting checkpoint folder...")
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            if os.path.isdir(folder_path):
                # Deletes folder if exits:
                rmtree(folder_path)
                # Creates new folder:
                print("Folder deleted. \n")
            else:
                print("Folder deleted. \n")
                pass
