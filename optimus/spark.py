from pyspark.sql import SparkSession
from functools import lru_cache

import os

# Messages strings
STARTING = "Starting or getting SparkSession and SparkContext..."
CHECK_POINT_CONFIG = "Setting checkpoint folder (local). If you are in a cluster change it with set_check_point_ " \
                     "folder(path,'hadoop')."
SUCCESS = "Optimus successfully imported. Have fun :)."


class Spark:
    def __init__(self, master="local", app_name="optimus"):
        """
        Initialize Optimus and create the Spark Dataframe
        :param master:
        :param app_name:
        """

        if master is not None:
            assert isinstance(master, str), "Error: master must be a string"
        if app_name:
            assert isinstance(app_name, str), "Error: app_name must be a string"

        self.master = master
        self.app_name = app_name
        print("""
             ____        __  _                     
            / __ \____  / /_(_)___ ___  __  _______
           / / / / __ \/ __/ / __ `__ \/ / / / ___/
          / /_/ / /_/ / /_/ / / / / / / /_/ (__  ) 
          \____/ .___/\__/_/_/ /_/ /_/\__,_/____/  
              /_/                                  
              """)

        print("Just checking that all necessary environments vars are present...")
        print("-----")
        print("PYSPARK_PYTHON=" + os.environ.get("PYSPARK_PYTHON"))
        print("SPARK_HOME=" + os.environ.get("SPARK_HOME"))
        print("JAVA_HOME=" + os.environ.get("JAVA_HOME"))
        print("-----")
        print(STARTING)
        self.get_ss()
        print(CHECK_POINT_CONFIG)

    @lru_cache(maxsize=None)
    def get_ss(self):
        """
        Return the Spark Session
        :return: None
        """

        return (SparkSession
                .builder
                # .enableHiveSupport()
                .master(self.master)
                .appName(self.app_name)
                .getOrCreate())

    def get_sc(self):
        """
        Return the Spark Context
        :return:
        """
        return self.get_ss().sparkContext
