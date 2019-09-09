import os
import platform
import sys
from shutil import rmtree

from deepdiff import DeepDiff
from pyspark.sql import DataFrame

from optimus.bumblebee import Comm
from optimus.dataframe.create import Create
from optimus.enricher import Enricher
from optimus.helpers.constants import *
from optimus.helpers.converter import val_to_list
from optimus.helpers.functions import absolute_path
from optimus.helpers.functions import append as append_df
from optimus.helpers.logger import logger
from optimus.helpers.output import print_json
from optimus.helpers.raiseit import RaiseIt
from optimus.io.jdbc import JDBC
from optimus.io.load import Load
from optimus.ml.models import ML
from optimus.profiler.profiler import Profiler
from optimus.server.server import Server
from optimus.spark import Spark
from optimus.version import __version__

# Singletons
Spark.instance = None
Profiler.instance = None
Comm.instance = None


class Optimus:
    __version__ = __version__
    cache = False

    def __init__(self, session=None, master="local[*]", app_name="optimus", checkpoint=False, path=None,
                 file_system="local",
                 verbose=False,
                 server=False,
                 repositories=None,
                 packages=None,
                 jars=[],
                 driver_class_path=[],
                 options=None,
                 additional_options=None,
                 comm=None,
                 load_avro=False,
                 cache=True
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

        Optimus.cache = cache

        if comm is True:
            Comm.instance = Comm()
        # else:
        #     Comm.instance = comm

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

            jdbc_jars = ["/jars/RedshiftJDBC42-1.2.16.1027.jar", "/jars/mysql-connector-java-8.0.16.jar",
                         "/jars/ojdbc8.jar", "/jars/postgresql-42.2.5.jar", "/jars/presto-jdbc-0.224.jar"]

            self._add_jars(absolute_path(jdbc_jars, "uri"))
            self._add_driver_class_path(absolute_path(jdbc_jars, "posix"))

            self._create_session()

            if path is None:
                path = os.getcwd()

            if checkpoint is True:
                self._set_check_point_folder(path, file_system)

        else:
            # If a session is passed by arguments just save the reference
            # logger.print("Spark session")
            Spark.instance = Spark().load(session)

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

        # Pickling
        # Spark.instance.sc.addPyFile(absolute_path("/helpers/pickle.py"))

        if server:
            logger.print("Starting Optimus Server...")
            s = Server()
            s.start()
            self.server_instance = s

        logger.print(SUCCESS)

        self.create = Create()
        self.load = Load()
        self.read = self.spark.read

        # Create singleton profiler
        Profiler.instance = Profiler()
        self.profiler = Profiler.instance
        self.ml = ML()

        # Set global output as html
        self.output("html")

    @staticmethod
    def connect(db_type="redshift", host=None, database=None, user=None, password=None, port=None, schema="public",
                oracle_tns=None, oracle_service_name=None, oracle_sid=None, presto_catalog=None):
        """
        Create the JDBC string connection
        :return: JDBC object
        """

        return JDBC(db_type, host, database, user, password, port, schema, oracle_tns, oracle_service_name, oracle_sid,
                    presto_catalog)

    def enrich(self, host="localhost", port=27017, username=None, password=None, db_name="jazz",
               collection_name="data"):
        """
        Create a enricher object
        :param host: url to mongodb
        :param port: port used my mongodb
        :param username: database username
        :param password: database password
        :param db_name: db user by the enricher
        :param collection_name: collection used by the enricher
        :return:
        """
        return Enricher(op=self, host=host, port=port, username=username, password=password, db_name=db_name,
                        collection_name=collection_name)

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
            Optimus.delete_check_point_folder(path=path, file_system=file_system)

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
            self.packages.append(p)

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

        ## Get python.exe fullpath
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
        if method is "json":
            diff = DeepDiff(df1.to_json(), df2.to_json(), ignore_order=False)
            print_json(diff)
        elif method is "collect":
            if df1.collect() == df2.collect():
                print("Dataframes are equal")
                return True
            else:
                print("Dataframes not equal. Use 'json' param to check for diffrences")
                return False

        else:
            RaiseIt.type_error(method, ["json", "collect"])
