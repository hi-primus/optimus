from pyspark.sql import SparkSession

from optimus.helpers.constants import *
from optimus.helpers.functions import is_pyarrow_installed, check_env_vars


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
        check_env_vars(["SPARK_HOME", "HADOOP_HOME", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON", "JAVA_HOME"])

        if is_pyarrow_installed() is True:
            logging.info("Pyarrow Installed")
        else:
            logging.info(
                "Pyarrow not installed. Pandas UDF not available. Install using 'pip install pyarrow'")
        logging.info("-----")
        logging.info(STARTING_SPARK)

        # Build the spark session
        self._spark = (SparkSession
                       .builder
                       .master(self.master)
                       .appName(self.app_name)
                       .getOrCreate())

    @property
    def spark(self):
        """
        Return the Spark Session
        :return: None
        """

        return self._spark

    @property
    def sc(self):
        """
        Return the Spark Context
        :return:
        """
        return self._spark.sparkContext
