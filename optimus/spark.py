from pyspark.sql import SparkSession

from optimus.helpers.constants import *
from optimus.helpers.functions import is_pyarrow_installed, check_env_vars

import optimus.helpers.logger as logger


class Spark:
    @property
    def spark(self):
        """
        Return the Spark Session
        :return: None
        """

        return self._spark

    def __init__(self, master="local[*]", app_name="optimus"):
        """

        :param master: Sets the Spark master URL to connect to, such as 'local' to run locally, 'local[4]' to run
        locally with 4 cores, or spark://master:7077 to run on a Spark standalone cluster.
        :param app_name: Sets a name for the application, which will be shown in the Spark web UI
        """

        self.master = master
        self.app_name = app_name

        logger.info(message=JUST_CHECKING)
        logger.info("-----")
        check_env_vars(["SPARK_HOME", "HADOOP_HOME", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON", "PYSPARK_SUBMIT_ARGS",
                        "JAVA_HOME"])

        if is_pyarrow_installed() is True:
            logger.info("Pyarrow Installed")
        else:
            logger.info(
                "Pyarrow not installed. Pandas UDF not available. Install using 'pip install pyarrow'")
        logger.info("-----")
        logger.info(STARTING_SPARK)

        # Build the spark session
        self._spark = SparkSession.builder \
            .master(master) \
            .config("spark.executor.heartbeatInterval", "110") \
            .appName(app_name) \
            .getOrCreate()

        # .option("driver", "org.postgresql.Driver")
        self._sc = self._spark.sparkContext
        # print(self._sc._conf.getAll())

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
        return self.sc.defaultParallelism

    @property
    def executors(self):
        """
        Get the number of executors. If launched in local mode executors in None
        :return:
        """
        return self.sc._conf.get('spark.executor.instances')
