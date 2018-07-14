from functools import lru_cache
import os

from pyspark.sql import SparkSession

from optimus.helpers.constants import *


class Spark:
    def __init__(self, master="local", app_name="optimus"):

        if master is not None:
            assert isinstance(master, str), "Error: master must be a string"
        if app_name is not None:
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

        print(JUST_CHECKING)
        print("-----")
        print("PYSPARK_PYTHON=" + os.environ.get("PYSPARK_PYTHON"))
        print("SPARK_HOME=" + os.environ.get("SPARK_HOME"))
        print("JAVA_HOME=" + os.environ.get("JAVA_HOME"))
        print("-----")
        print(STARTING)
        self.get_ss()
        print(CHECK_POINT_CONFIG)

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
