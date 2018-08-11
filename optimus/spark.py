import os
from functools import lru_cache

from pyspark.sql import SparkSession

from optimus.helpers.constants import *
from optimus.helpers.functions import is_pyarrow_installed


class Spark:
    def __init__(self, master="local[*]", app_name="optimus"):
        """

        :param master: Sets the Spark master URL to connect to, such as “local” to run locally, “local[4]” to run
        locally with 4 cores, or “spark://master:7077” to run on a Spark standalone cluster.
        :param app_name: Sets a name for the application, which will be shown in the Spark web UI
        """

        self.master = master
        self.app_name = app_name

        logging.info(JUST_CHECKING)
        logging.info("-----")
        try:
            logging.info("PYSPARK_PYTHON=" + os.environ.get("PYSPARK_PYTHON"))
        except:
            logging.info("You don't have PYSPARK_PYTHON set")
        try:
            logging.info("SPARK_HOME=" + os.environ.get("SPARK_HOME"))
        except:
            logging.info("You don't have SPARK_HOME set")
        try:
            logging.info("JAVA_HOME=" + os.environ.get("JAVA_HOME"))
        except:
            logging.info("You don't have JAVA_HOME set")
        if is_pyarrow_installed() is True:
            logging.info("Pyarrow Installed")
        else:
            logging.info("Pyarrow not installed. Pandas UDF not available. Install using 'pip install pyarrow'")
        logging.info("-----")
        logging.info(STARTING_SPARK)
        self.spark()
        # self.get_sc().addPyFile("../optimus/helpers/checkit.py")

    @lru_cache(maxsize=None)
    def spark(self):
        """
        Return the Spark Session
        :return: None
        """

        return (SparkSession
                .builder
                .enableHiveSupport()
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
