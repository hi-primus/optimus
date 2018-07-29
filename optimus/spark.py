# from functools import lru_cache
import os
import logging

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

        logging.info("""
             ____        __  _                     
            / __ \____  / /_(_)___ ___  __  _______
           / / / / __ \/ __/ / __ `__ \/ / / / ___/
          / /_/ / /_/ / /_/ / / / / / / /_/ (__  ) 
          \____/ .___/\__/_/_/ /_/ /_/\__,_/____/  
              /_/                                  
              """)

        logging.info(JUST_CHECKING)
        logging.info("-----")
        logging.info("PYSPARK_PYTHON=" + os.environ.get("PYSPARK_PYTHON"))
        logging.info("SPARK_HOME=" + os.environ.get("SPARK_HOME"))
        logging.info("JAVA_HOME=" + os.environ.get("JAVA_HOME"))
        logging.info("-----")
        logging.info(STARTING)
        self.get_ss()

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
