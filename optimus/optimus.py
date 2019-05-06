import os
import sys
from shutil import rmtree

from deepdiff import DeepDiff  # For Deep Difference of 2 objects
from pyspark.sql import DataFrame

from optimus.enricher import Enricher
from optimus.functions import append, Create
from optimus.helpers.constants import *
from optimus.helpers.convert import val_to_list
from optimus.helpers.functions import print_html, print_json
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.io.jdbc import JDBC
from optimus.io.load import Load
from optimus.ml.models import ML
from optimus.profiler.profiler import Profiler
from optimus.server.server import Server
from optimus.spark import Spark

Spark.instance = None


class Optimus:

    def __init__(self, session=None, master="local[*]", app_name="optimus", checkpoint=False, path=None,
                 file_system="local",
                 verbose=False, dl=False,
                 server=False,
                 repositories=None,
                 packages=None,
                 jars=None,
                 options=None,
                 additional_options=None,
                 queue_url=None,
                 queue_exchange=None,
                 queue_routing_key="optimus"
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
        if session is None:
            # print("Creating Spark Session...")
            # If a Spark session in not passed by argument create it

            self.master = master
            self.app_name = app_name

            if options is None:
                options = {}

            self.options = options

            if packages is None:
                packages = []
            else:
                packages = val_to_list(packages)

            self.packages = packages
            self.repositories = repositories

            if jars is None:
                jars = {}

            self.jars = jars
            self.additional_options = additional_options

            self.verbose(verbose)

            # Load Avro.
            # TODO:
            #  if the Spark 2.4 version is going to be used this is not neccesesary.
            #  Maybe we can check a priori which version fo Spark is going to be used
            # self._add_spark_packages(["com.databricks:spark-avro_2.11:4.0.0"])

            if dl is True:
                self._add_spark_packages(
                    ["databricks:spark-deep-learning:1.5.0-spark2.4-s_2.11"])

                self._start_session()

                from optimus.dl.models import DL
                self.dl = DL()
            else:
                self._start_session()

            if path is None:
                path = os.getcwd()

            if checkpoint is True:
                self._set_check_point_folder(path, file_system)

        else:
            # If a session is passed by arguments just save the reference

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

        if server:
            logger.print("Starting Optimus Server...")
            s = Server()
            s.start()
            self.server_instance = s

        logger.print(SUCCESS)

        self.create = Create()
        self.load = Load()
        self.read = self.spark.read
        self.profiler = Profiler(
            queue_url=queue_url,
            queue_exchange=queue_exchange,
            queue_routing_key=queue_routing_key
        )
        self.ml = ML()

        #
        self._load_css()

        # Set global output as html
        self.output("html")

    @staticmethod
    def _load_css():
        """
        Try to load the css for templates
        :return:
        """
        try:
            if __IPYTHON__:
                # # Load the Jinja template

                path = os.path.dirname(os.path.abspath(__file__))
                url = path + "//css//styles.css"
                styles = open(url, "r").read()
                s = '<style>%s</style>' % styles
                print_html(s)
        except NameError:
            pass

    @staticmethod
    def connect(db_type="redshift", url=None, database=None, user=None, password=None, port=None):
        """
        Create the JDBC string connection
        :return:
        """
        return JDBC(db_type, url, database, user, password, port)

    def enrich(self, host="localhost", port=27017):
        """
        :param host:
        :param port:
        :return:
        """
        return Enricher(op=self, host=host, port=port, )

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
    def append(dfs, like):
        """
        Concat multiple dataframes
        :param dfs: List of Dataframes
        :param like: concat as columns or rows
        :return:
        """
        return append(dfs, like)

    def _add_spark_packages(self, packages):
        """
        Define the Spark packages that must be loaded at start time
        :param packages:
        :return:
        """
        for p in packages:
            self.packages.append(p)

    def _setup_repositories(self):
        if self.repositories:
            return '--repositories {}'.format(','.join(self.repositories))
        else:
            return ''

    def _setup_packages(self):
        if self.packages:
            return '--packages {}'.format(','.join(self.packages))
        else:
            return ''

    def _setup_jars(self):
        if self.jars:
            return '--jars {}'.format(','.join(self.jars))
        else:
            return ''

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

    def _start_session(self):
        """
        Start a Spark session using jar, packages, repositories and options given
        :return:
        """
        ## Get python.exe fullpath
        os.environ['PYSPARK_PYTHON'] = sys.executable

        submit_args = [
            # options that were already defined through PYSPARK_SUBMIT_ARGS
            # take precedence over SparklySession's
            os.environ.get('PYSPARK_SUBMIT_ARGS', '').replace('pyspark-shell', ''),
            self._setup_repositories(),
            self._setup_packages(),
            self._setup_jars(),
            self._setup_options(self.additional_options),
            'pyspark-shell',
        ]

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
            assert (df1.collect() == df2.collect())
        else:
            RaiseIt.type_error(method, ["json", "collect"])
