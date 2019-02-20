from pyspark.sql import SparkSession

from optimus.helpers.constants import *
from optimus.helpers.functions import is_pyarrow_installed, check_env_vars


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

        import os
        # env = '--jars postgresql-42.2.5.jar pyspark-shell'
        os.environ['PYSPARK_SUBMIT_ARGS'] = ''

        # print(os.environ['PYSPARK_SUBMIT_ARGS'])
        # print(os.environ)

        # Build the spark session
        self._spark = SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .config("spark.jars", "RedshiftJDBC42-1.2.16.1027.jar") \
            .getOrCreate()

        # .config("spark.jars", "postgresql-42.2.5.jar") \
        # print(self._spark.conf.getAll())

        # .option("driver", "org.postgresql.Driver")
        self._sc = self._spark.sparkContext
        # print(self._sc._conf.getAll())
        # SparkContext.sc._conf

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
