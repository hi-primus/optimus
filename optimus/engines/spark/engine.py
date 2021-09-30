import os
import platform
import sys
from shutil import rmtree

from deepdiff import DeepDiff
from optimus._version import __version__
from pyspark.sql import DataFrame

import optimus.helpers.functions_spark
from optimus.engines.base.engine import BaseEngine
from optimus.engines.spark.create import Create
from optimus.engines.spark.io.jdbc import JDBC
from optimus.engines.spark.io.load import Load
from optimus.engines.spark.spark import Spark
from optimus.helpers.constants import *
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import absolute_path
from optimus.helpers.functions_spark import append as append_df
from optimus.helpers.logger import logger
from optimus.helpers.output import output_json
from optimus.helpers.raiseit import RaiseIt
from optimus.optimus import Engine, EnginePretty

# Singletons
Spark.instance = None


class SparkEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, session=None, master="local[*]", app_name="optimus", checkpoint=False, path=None,
                 file_system="local",
                 verbose=False,
                 repositories=None,
                 packages=None,
                 jars=None,
                 driver_class_path=None,
                 options=None,
                 additional_options=None,
                 load_avro=False,
                 ):

        """
        Transform and roll out
        :param master: 'Master', 'local' or ip address to a cluster
        :param app_name: Spark app name
        :param path: path to the checkpoint folder
        :param checkpoint: If True create a checkpoint folder
        :param file_system: 'local' or 'hadoop'
        :param additional_options:


        :param options: Configuration options that are passed to spark-submit.
            See `the list of possible options
            <https://spark.apache.org/docs/2.4.1/configuration.html#available-properties>`_.
            Note that any options set already through PYSPARK_SUBMIT_ARGS will override
            these.
        :type options: (dict[str,str])
        :param repositories: List of additional maven repositories for package lookup.
        :type repositories: (list[str])

        :param packages: Spark packages that should be installed.
        :type packages: (list[str])

        :param jars: Full paths to jar files that we want to include to the session.
        :type jars: (list[str])

        """
        self.preserve = False

        if jars is None:
            jars = []

        if driver_class_path is None:
            driver_class_path = []

        if session is None:
            # Creating Spark Session
            # If a Spark session in not passed by argument create one

            self.master = master
            self.app_name = app_name

            if options is None:
                options = {}

            self.options = options

            # Initialize as lists
            self.packages = val_to_list(packages)
            self.repositories = val_to_list(repositories)
            self.jars = val_to_list(jars)
            self.driver_class_path = val_to_list(driver_class_path)

            self.additional_options = additional_options

            self.verbose(verbose)

            # Because avro depends of a external package you can decide if should be loaded
            if load_avro == "2.4":
                self._add_spark_packages(["org.apache.spark:spark-avro_2.12:2.4.3"])

            elif load_avro == "2.3":
                self._add_spark_packages(["com.databricks:spark-avro_2.11:4.0.0"])

            # jdbc_jars = ["/jars/spark-redis-2.4.1-SNAPSHOT-jar-with-dependencies.jar",
            #              "/jars/RedshiftJDBC42-1.2.16.1027.jar",
            #              "/jars/mysql-connector-java-8.0.16.jar",
            #              "/jars/ojdbc8.jar", "/jars/postgresql-42.2.5.jar", "/jars/presto-jdbc-0.224.jar",
            #              "/jars/spark-cassandra-connector_2.11-2.4.1.jar", "/jars/sqlite-jdbc-3.27.2.1.jar",
            #              "/jars/mssql-jdbc-7.4.1.jre8.jar"]
            #
            # self._add_jars(absolute_path(jdbc_jars, "uri"))
            # self._add_driver_class_path(absolute_path(jdbc_jars, "posix"))

            self._create_session()

            if path is None:
                path = os.getcwd()

            if checkpoint is True:
                self._set_check_point_folder(path, file_system)

        else:
            # If a session is passed by arguments just save the reference
            # logger.print("Spark session")
            Spark.instance = Spark().load(session)

        self.client = Spark.instance._spark

        # Initialize Spark
        logger.print("""
                             ____        __  _
                            / __ \____  / /_(_)___ ___  __  _______
                           / / / / __ \/ __/ / __ `__ \/ / / / ___/
                          / /_/ / /_/ / /_/ / / / / / / /_/ (__  )
                          \____/ .___/\__/_/_/ /_/ /_/\__,_/____/
                              /_/
                              """)
        logger.print(STARTING_OPTIMUS)
        logger.print(SUCCESS)

    @property
    def F(self):
        from optimus.engines.spark.functions import SparkFunctions
        return SparkFunctions(self)

    @property
    def create(self):
        return Create(self)

    @property
    def load(self):
        return Load(self)

    @property
    def engine(self):
        return Engine.SPARK.value

    @property
    def engine_label(self):
        return EnginePretty.SPARK.value

    @staticmethod
    def connect(driver=None, host=None, database=None, user=None, password=None, port=None, schema="public",
                oracle_tns=None, oracle_service_name=None, oracle_sid=None, presto_catalog=None,
                cassandra_keyspace=None, cassandra_table=None):
        """
        Create the JDBC string connection
        :return: JDBC object
        """

        return JDBC(host, database, user, password, port, driver, schema, oracle_tns, oracle_service_name, oracle_sid,
                    presto_catalog, cassandra_keyspace, cassandra_table)

    @staticmethod
    def output(output):
        """
        Change the table output style
        :param output: "ascii" or "html"
        :return:
        """
        DataFrame.output = output

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

    @staticmethod
    def stop():
        """
        Stop Spark Session
        :return:
        """
        Spark.instance.spark.stop()

    @staticmethod
    def _set_check_point_folder(path, file_system):
        """
        Function that receives a workspace path where a folder is created.
        This folder will store temporal dataframes when user writes the .checkPoint().

        :param path: Location of the dataset (string).
        :param file_system: Describes if file system is local or hadoop file system.

        """

        print_check_point_config(file_system)

        if file_system == "hadoop":
            folder_path = path + "/" + "checkPointFolder"
            SparkEngine.delete_check_point_folder(path=path, file_system=file_system)

            # Creating file:
            logger.print("Creating the hadoop folder...")
            command = "hadoop fs -mkdir " + folder_path
            logger.print("$" + command)
            os.system(command)
            logger.print("Hadoop folder created. \n")

            logger.print("Setting created folder as checkpoint folder...")
            Spark.instance.sc.setCheckpointDir(folder_path)
        elif file_system == "local":
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            logger.print("Deleting previous folder if exists...")
            if os.path.isdir(folder_path):
                # Deletes folder if exits:
                rmtree(folder_path)

            logger.print("Creating the checkpoint directory...")
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
            logger.print("Deleting checkpoint folder...")
            command = "hadoop fs -rm -r " + folder_path
            os.system(command)
            logger.print("$" + command)
            logger.print("Folder deleted.")
        elif file_system == "local":
            logger.print("Deleting checkpoint folder...")
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            if os.path.isdir(folder_path):
                # Deletes folder if exits:
                rmtree(folder_path)
                # Creates new folder:
                logger.print("Folder deleted.")
            else:
                logger.print("Folder deleted.")
        else:
            RaiseIt.value_error(file_system, ["hadoop", "local"])

    @staticmethod
    def append(dfs, like="columns"):
        """
        Append dataframes rows or columns wise
        :param dfs: Dataframes
        :param like: 'columns' or 'rows'
        :return:
        """
        return append_df(dfs, like)

    def _setup_repositories(self):
        if self.repositories:
            return '--repositories {}'.format(','.join(self.repositories))
        else:
            return ''

    # Spark Package
    def _add_spark_packages(self, packages):
        """
        Define the Spark packages that must be loaded at start time
        :param packages:
        :return:
        """

        for p in val_to_list(packages):
            optimus.helpers.functions_spark.append(p)

    def _setup_packages(self):
        if self.packages:
            return '--packages {}'.format(','.join(self.packages))
        else:
            return ''

    # Jar
    def _add_jars(self, jar):
        for j in val_to_list(jar):
            self.jars.append(j)

    def _setup_jars(self):
        if self.jars:
            return '--jars "{}"'.format(','.join(self.jars))
        else:
            return ''

    # Driver class path
    def _add_driver_class_path(self, driver_class_path):

        for d in val_to_list(driver_class_path):
            self.driver_class_path.append(d)

    def _setup_driver_class_path(self):

        p = platform.system()
        logger.print("Operative System:" + p)
        if p == "Windows":
            separator = ";"
        else:  # for p == "Linux" or p == "Darwin" or any other p == value
            separator = ":"

        if self.driver_class_path:
            return '--driver-class-path "{}"'.format(separator.join(self.driver_class_path))
        else:
            return ''

    # Options
    def _setup_options(self, additional_options):
        options = {}

        options.update(self.options)

        if additional_options:
            options.update(additional_options)

        if 'spark.sql.catalogImplementation' not in options:
            options['spark.sql.catalogImplementation'] = 'hive'

        # Here we massage conf properties with the intent to pass them to
        # spark-submit; this is convenient as it is unified with the approach
        # we take for repos, packages and jars, and it also handles precedence
        # of conf properties already defined by the user in a very
        # straightforward way (since we always append to PYSPARK_SUBMIT_ARGS)
        return ' '.join('--conf "{}={}"'.format(*o) for o in sorted(options.items()))

    def _create_session(self):
        """
        Start a Spark session using jar, packages, repositories and options given
        :return:
        """

        # Get python.exe fullpath
        os.environ['PYSPARK_PYTHON'] = sys.executable

        # Remove duplicated strings
        # print(self._setup_jars())
        # print(os.environ.get('PYSPARK_SUBMIT_ARGS', '').replace(self._setup_jars(), ''))

        submit_args = [
            # options that were already defined through PYSPARK_SUBMIT_ARGS
            # take precedence over SparklySession's

            self._setup_repositories(),
            self._setup_packages(),
            self._setup_jars(),
            self._setup_driver_class_path(),
            self._setup_options(self.additional_options),
            'pyspark-shell',
        ]

        if self.preserve:
            submit_args.insert(0, os.environ.get('PYSPARK_SUBMIT_ARGS', '').replace('pyspark-shell', ''))

        env = ' '.join(filter(None, submit_args))
        os.environ['PYSPARK_SUBMIT_ARGS'] = env
        Spark.instance = Spark().create(self.master, self.app_name)

    def has_package(self, package_prefix):
        """
        Check if the package is available in the session.
        :param package_prefix: E.g. "org.elasticsearch:elasticsearch-spark".
        :type package_prefix: str
        :return bool
        """
        return any(package for package in self.packages if package.startswith(package_prefix))

    def has_jar(self, jar_name):
        """
        Check if the jar is available in the session.
        :param jar_name: E.g. "mysql-connector-java".
        :type jar_name: str
        :return: bool
        """
        return any(jar for jar in self.jars if jar_name in jar)

    @staticmethod
    def verbose(verbose):
        """
        Enable verbose mode
        :param verbose:
        :return:
        """

        logger.active(verbose)

    @staticmethod
    def compare(df1, df2, method="json"):
        """
        Compare 2 Spark dataframes
        :param df1:
        :param df2:
        :param method: json or a
        :return:
        """
        if method == "json":
            diff = DeepDiff(df1.to_json(), df2.to_json(), ignore_order=False)
            print(output_json(diff))
        elif method == "collect":
            if df1.collect() == df2.collect():
                print("Dataframes are equal")
                return True
            else:
                print("Dataframes not equal. Use 'json' param to check for differences")
                return False

        else:
            RaiseIt.type_error(method, ["json", "collect"])
