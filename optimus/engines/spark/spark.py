from pyspark.sql import SparkSession

from optimus.engines.spark.constants import STARTING_SPARK
from optimus.helpers.constants import *
from optimus.helpers.functions import is_pyarrow_installed, check_env_vars
from optimus.helpers.logger import logger


class Spark:
    def __init__(self):
        self._spark = None
        self._sc = None

    def create(self, master="local[*]", app_name="optimus"):
        """

        :param master: Sets the Spark master URL to connect to, such as 'local' to run locally, 'local[4]' to run
        locally with 4 cores, or spark://master:7077 to run on a Spark standalone cluster.
        :param app_name: Sets a name for the application, which will be shown in the Spark web UI
        """

        logger.print(JUST_CHECKING)
        logger.print("-----")
        check_env_vars(["SPARK_HOME", "HADOOP_HOME", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON", "PYSPARK_SUBMIT_ARGS",
                        "JAVA_HOME"])

        if is_pyarrow_installed() is True:
            logger.print("Pyarrow Installed")
        else:
            logger.print(
                "Pyarrow not installed. Pandas UDF not available. Install using 'pip install pyarrow'")
        logger.print("-----")
        logger.print(STARTING_SPARK)

        # Build the spark session
        self._spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .getOrCreate()

        self._sc = self._spark.sparkContext
        logger.print("Spark Version:" + self._sc.version)

        return self

    def load(self, session):
        self._spark = session
        self._sc = session.sparkContext
        return self

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
        return self._sc

    @property
    def parallelism(self):
        """
        Returns default level of parallelism defined on SparkContext. By default it is number of cores available.
        :param self: Dataframe
        :return:
        """
        return self._sc.defaultParallelism

    @property
    def executors(self):
        """
        Get the number of executors. If launched in local mode executors in None
        :return:
        """
        return self._sc._conf.get('spark.executor.instances')
