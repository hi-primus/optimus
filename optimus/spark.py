# from functools import lru_cache
import os
import logging
from functools import lru_cache

from pyspark.sql import SparkSession

from optimus.helpers.constants import *
from optimus.helpers.functions import is_str


class Spark:
    def __init__(self, master="local", app_name="optimus"):
        """

        :param master: Sets the Spark master URL to connect to, such as “local” to run locally, “local[4]” to run
        locally with 4 cores, or “spark://master:7077” to run on a Spark standalone cluster.
        :param app_name: Sets a name for the application, which will be shown in the Spark web UI
        """

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
        self.spark()
        #self.get_sc().addPyFile("../optimus/helpers/checkit.py")
        #self.get_sc().addPyFile("../optimus/spark.py")
        #self.get_sc().addPyFile("../optimus/optimus.py")

    @lru_cache(maxsize=None)
    def spark(self):
        """
        Return the Spark Session
        :return: None
        """

        return (SparkSession
                .builder
                # .enableHiveSupport()
                .master(self.master)
                .appName(self.app_name)
                .getOrCreate()
                )

    def sc(self):
        """
        Return the Spark Context
        :return:
        """
        return self.spark().sparkContext
