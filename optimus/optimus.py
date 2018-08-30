import logging
import os
from shutil import rmtree

from optimus.create import Create
from optimus.functions import concat
from optimus.helpers.constants import *
from optimus.helpers.raiseit import RaiseIt
from optimus.io.load import Load
from optimus.ml.models import ML
from optimus.profiler.profiler import Profiler
from optimus.spark import Spark

Spark.instance = None


class Optimus:

    def __init__(self, master="local[*]", app_name="optimus", checkpoint=False, path=None, file_system="local",
                 verbose=False, dl=False):

        """
        Transform and roll out
        :param master: 'Master', 'local' or ip address to a cluster
        :param app_name: Spark app name
        :param path: path to the checkpoint folder
        :param checkpoint: If True create a checkpoint folder
        :param file_system: 'local' or 'hadoop'
        """

        if verbose is True:
            logging.basicConfig(format="%(message)s", level=logging.INFO)
        elif verbose is False:
            logging.propagate = False
            logging.disable(logging.NOTSET)

        if dl is True:
            Optimus.add_spark_packages(["databricks:spark-deep-learning:1.1.0-spark2.3-s_2.11 pyspark-shell"])

            Spark.instance = Spark(master, app_name)
            from optimus.dl.models import DL
            self.dl = DL()
        else:

            Spark.instance = Spark(master, app_name)
            pass

        if path is None:
            path = os.getcwd()

        # Initialize Spark
        logging.info("""
                             ____        __  _                     
                            / __ \____  / /_(_)___ ___  __  _______
                           / / / / __ \/ __/ / __ `__ \/ / / / ___/
                          / /_/ / /_/ / /_/ / / / / / / /_/ (__  ) 
                          \____/ .___/\__/_/_/ /_/ /_/\__,_/____/  
                              /_/                                  
                              """)

        logging.info(STARTING_OPTIMUS)
        if checkpoint is True:
            self.set_check_point_folder(path, file_system)

        logging.info(SUCCESS)

        self.create = Create()
        self.load = Load()
        self.read = self.spark.read
        self.profiler = Profiler()
        self.ml = ML()

    @property
    def spark(self):
        """
        Return a Spark session object
        :return:
        """
        return Spark.instance.spark

    @property
    def sc(self):
        """
        Return a Spark Context object
        :return:
        """
        return Spark.instance.sc

    def stop(self):
        """
        Stop Spark Session
        :return:
        """
        Spark.instance.spark.stop()

    @staticmethod
    def add_spark_packages(packages):
        """
        Define the Spark packages that must be loaded at start time
        :param packages:
        :return:
        """
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages " + " ".join(packages)

    @staticmethod
    def set_check_point_folder(path, file_system):
        """
        Function that receives a workspace path where a folder is created.
        This folder will store temporal dataframes when user writes the .checkPoint().

        :param path: Location of the dataset (string).
        :param file_system: Describes if file system is local or hadoop file system.

        """

        print_check_point_config(file_system)

        if file_system == "hadoop":
            folder_path = path + "/" + "checkPointFolder"
            Optimus.delete_check_point_folder(path=path, file_system=file_system)

            # Creating file:
            logging.info("Creating the hadoop folder...")
            command = "hadoop fs -mkdir " + folder_path
            logging.info("$" + command)
            os.system(command)
            logging.info("Hadoop folder created. \n")

            logging.info("Setting created folder as checkpoint folder...")
            Spark.instance.sc.setCheckpointDir(folder_path)
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

            Spark.instance.sc.setCheckpointDir(dirName="file:///" + folder_path)
        else:
            RaiseIt.value_error(file_system, ["hadoop", "local"])

    @staticmethod
    def delete_check_point_folder(path, file_system):
        """
        Function that deletes the temporal folder where temp files were stored.
        The path required is the same provided by user in setCheckPointFolder().

        :param path: path where the info will be saved
        :param file_system: Describes if file system is local or hadoop file system.
        :return:
        """

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
        else:
            RaiseIt.value_error(file_system, ["hadoop", "local"])

    @staticmethod
    def concat(dfs, like):
        return concat(dfs, like)
