# -*- coding: utf-8 -*-
# Importing os module for system operative utilities
import os
# Importing SparkSession:
from pyspark.sql.session import SparkSession
# Importing module to delete folders
from shutil import rmtree
# Importing module to work with urls
import urllib.request
# Importing module for regex
import re
# Importing SparkContext
import pyspark
# URL reading
import tempfile
from urllib.request import Request, urlopen


class Utilities:
    def __init__(self):

        # Setting spark as a global variable of the class
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        # Setting SparkContent as a global variable of the class
        self.__sc = self.spark.sparkContext
        # Set empty container for url
        self.url = ""

    def create_data_frame(self, data, names):
        """
        Create a spark Dataframe from a list of tuples. This will infer the type for each column.
        :param data: List of tuples with data
        :param names: List of names for the columns
        :return: Spark dataframe
        """

        assert isinstance(data, list) and isinstance(data[0], tuple), \
            "data should be a list of tuples"

        assert isinstance(names, list) and isinstance(names[0], str), \
            "names should be a list of strings"

        return self.spark.createDataFrame(data, names)

    def read_csv(self, path, sep=',', header='true'):
        """This funcion read a dataset from a csv file.

        :param path     Path or location of the file.
        :param sep   Usually delimiter mark are ',' or ';'.
        :param  header:     Tell the function whether dataset has a header row.

        :return dataFrame
        """
        assert ((header == 'true') or (header == 'false')), "Error, header argument must be 'true' or 'false'. " \
                                                            "header must be string dataType, i.e: header='true'"
        assert isinstance(sep, str), "Error, delimiter mark argumen must be string dataType."

        assert isinstance(path, str), "Error, path argument must be string datatype."

        return self.spark.read \
            .format('csv') \
            .options(header=header) \
            .options(delimiter=sep) \
            .options(inferSchema='true') \
            .load(path)

    def read_url(self, path=None, ty="csv"):
        """
        Reads dataset from URL.
        :param path: string for URL to read
        :param ty: type of the URL backend (can be csv or json)
        :return: pyspark dataframe from URL.
        """
        if "https://" in str(path) or "http://" in str(path) or "file://" in str(path):
            if ty is 'json':
                self.url = str(path)
                return self.json_load_spark_data_frame_from_url(str(path))
            else:
                return self.load_spark_data_frame_from_url(str(path))
        else:
            print("Unknown sample data identifier. Please choose an id from the list below")

    def json_data_loader(self, path):
        res = open(path, 'r').read()
        print("Loading file using a pyspark dataframe for spark 2")
        data_rdd = self.__sc.parallelize([res])
        return self.spark.read.json(data_rdd)

    def data_loader(self, path):

        print("Loading file using 'SparkSession'")
        csvload = self.spark.builder.getOrCreate() \
                .read \
                .format("csv") \
                .options(header=True) \
                .options(mode="DROPMALFORMED")

        return csvload.option("inferSchema", "true").load(path)

    def load_spark_data_frame_from_url(self, data_url):
        i = data_url.rfind('/')
        data_name = data_url[(i + 1):]
        data_def = {
            "displayName": data_name,
            "url": data_url
        }

        return Downloader(data_def).download(self.data_loader)

    def json_load_spark_data_frame_from_url(self, data_url):
        i = data_url.rfind('/')
        data_name = data_url[(i + 1):]
        data_def = {
            "displayName": data_name,
            "url": data_url
        }

        return Downloader(data_def).download(self.json_data_loader)

    def read_dataset_parquet(self, path):
        """This function allows user to read parquet files. It is import to clarify that this method is just based
        on the spark.read.parquet(path) Apache Spark method. Only assertion instructions has been added to
        ensure user has more hints about what happened when something goes wrong.
        :param  path    Path or location of the file. Must be string dataType.

        :return dataFrame"""
        assert isinstance(path, str), "Error: path argument must be string dataType."
        assert (("file:///" == path[0:8]) or ("hdfs:///" == path[0:8])), "Error: path must be with a 'file://' prefix \
        if the file is in the local disk or a 'path://' prefix if the file is in the Hadood file system"
        return self.spark.read.parquet(path)

    def csv_to_parquet(self, input_path, output_path, sep_csv, header_csv, num_partitions=None):
        """This method transform a csv dataset file into a parquet.

        The method reads a existing csv file using the inputPath, sep_csv and headerCsv arguments.

        :param  input_path   Address location of the csv file.
        :param  output_path  Address where the new parquet file will be stored.
        :param  sep_csv    Delimiter mark of the csv file, usually is ',' or ';'.
        :param  header_csv   This argument specifies if csv file has header or not.
        :param  num_partitions Specifies the number of partitions the user wants to write the dataset."""

        df = self.read_csv(input_path, sep_csv, header=header_csv)

        if num_partitions is not None:
            assert (num_partitions <= df.rdd.getNumPartitions()), "Error: num_partitions specified is greater that the" \
                                                                  "partitions in file store in memory."
            # Writing dataset:
            df.coalesce(num_partitions).write.parquet(output_path)

        else:
            df.write.parquet(output_path)

    def set_check_point_folder(self, path, file_system):
        """Function that receives a workspace path where a folder is created.
        This folder will store temporal
        dataframes when user writes the DataFrameTransformer.checkPoint().

        This function needs the sc parameter, which is the spark context in order to
        tell spark where is going to save the temporal files.

        It is recommended that users deleted this folder after all transformations are completed
        and the final dataframe have been saved. This can be done with deletedCheckPointFolder function.

        :param path     Location of the dataset (string).
        :param file_system   Describes if file system is local or hadoop file system.

        """

        assert (isinstance(file_system, str)), \
            "Error: file_system argument must be a string."

        assert (file_system == "hadoop") or (file_system == "local"), \
            "Error, file_system argument only can be 'local' or 'hadoop'"

        if file_system == "hadoop":
            folder_path = path + "/" + "checkPointFolder"
            self.delete_check_point_folder(path=path, file_system=file_system)

            # Creating file:
            print("Creation of hadoop folder...")
            command = "hadoop fs -mkdir " + folder_path
            print("$" + command)
            os.system(command)
            print("Hadoop folder created. \n")

            print("Setting created folder as checkpoint folder...")
            self.__sc.setCheckpointDir(folder_path)
            print("Done.")
        else:
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            print("Deleting previous folder if exists...")
            if os.path.isdir(folder_path):

                # Deletes folder if exits:
                rmtree(folder_path)
                print("Creation of checkpoint directory...")
                # Creates new folder:
                os.mkdir(folder_path)
                print("Done.")
            else:
                print("Creation of checkpoint directory...")

                # Creates new folder:
                os.mkdir(folder_path)
                print("Done.")

            self.__sc.setCheckpointDir(dirName="file:///" + folder_path)

    @classmethod
    def delete_check_point_folder(cls, path, file_system):
        """Function that deletes the temporal folder where temp files were stored.
        The path required is the same provided by user in setCheckPointFolder().

        :param file_system   Describes if file system is local or hadoop file system.
        """
        assert (isinstance(file_system, str)), "Error: file_system argument must be a string."

        assert (file_system == "hadoop") or (file_system == "local"), \
            "Error, file_system argument only can be 'local' or 'hadoop'"

        if file_system == "hadoop":
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            print("Deleting checkpoint folder...")
            command = "hadoop fs -rm -r " + folder_path
            os.system(command)
            print("$" + command)
            print("Folder deleted. \n")
        else:
            print("Deleting checkpoint folder...")
            # Folder path:
            folder_path = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            if os.path.isdir(folder_path):
                # Deletes folder if exits:
                rmtree(folder_path)
                # Creates new folder:
                print("Folder deleted. \n")
            else:
                print("Folder deleted. \n")
                pass

    @classmethod
    def get_column_names_by_type(cls, df, data_type):
        """This function returns column names of dataFrame which have the same
        datatype provided. It analyses column datatype by dataFrame.dtypes method.

        :return    List of column names of a type specified.
        """
        assert (data_type in ['string', 'integer', 'float', 'date', 'double']), \
            "Error, data_type only can be one of the following values: 'string', 'integer', 'float', 'date', 'double'"

        dicc = {'string': 'string', 'integer': 'int', 'float': 'float', 'double': 'double'}

        return list(y[0] for y in filter(lambda x: x[1] == dicc[data_type], df.dtypes))


class Downloader(object):
    def __init__(self, data_def):
        self.data_def = data_def
        self.headers = {"User-Agent": "PixieDust Sample Data Downloader/1.0"}

    def download(self, data_loader):
        display_name = self.data_def["displayName"]
        bytes_downloaded = 0
        if "path" in self.data_def:
            path = self.data_def["path"]
        else:
            url = self.data_def["url"]
            req = Request(url, None, self.headers)
            print("Downloading '{0}' from {1}".format(display_name, url))
            with tempfile.NamedTemporaryFile(delete=False) as f:
                bytes_downloaded = self.write(urlopen(req), f)
                path = f.name
                self.data_def["path"] = path = f.name
        if path:
            try:
                if bytes_downloaded > 0:
                   print("Downloaded {} bytes".format(bytes_downloaded))
                print("Creating {1} DataFrame for '{0}'. Please wait...".format(display_name, 'pySpark'))
                return data_loader(path)
            finally:
                print("Successfully created {1} DataFrame for '{0}'".format(display_name, 'pySpark'))

    @staticmethod
    def write(response, file, chunk_size=8192):
        total_size = response.headers['Content-Length'].strip() if 'Content-Length' in response.headers else 100
        total_size = int(total_size)
        bytes_so_far = 0

        while 1:
            chunk = response.read(chunk_size)
            bytes_so_far += len(chunk)
            if not chunk:
                break
            file.write(chunk)
            total_size = bytes_so_far if bytes_so_far > total_size else total_size

        return bytes_so_far


class Airtable:
    def __init__(self, path):
        # Setting airtable dataset variable
        self._air_table = None
        # Setting spark as a global variable of the class
        self.spark = SparkSession()
        self.sc = self.spark.sparkContext
        # Reading dataset
        self.read_air_table_csv(path)

    def read_air_table_csv(self, path):
        """This function reads the airtable or csv file that user have to fill when the original dataframe
        is analyzed.

        The path could be a dowload link or a path from a local directory.
        When a local directory is provided, it is necessary to write the corresponding prefix. i.e.:
        file://.... or hdfs://...
        """

        if re.match("https", path):
            print("Reading file from web...")
            # Doing a request to url:
            response = urllib.request.urlopen(path)
            # Read obtained data:
            data = response.read()  # a `bytes` object
            # Decoding data:
            text = data.decode('utf-8')
            # Here text is splitted by ',' adn replace
            values = [line.replace("\"", "").split(",") for line in text.split("\n")]

            # Airtable datafra that corresponds to airtable table:
            self._air_table = self.sc.parallelize(values[1:]).toDF(values[0])
            print("Done")

        else:
            print("Reading local file...")
            self._air_table = self.spark.read \
                .format('csv') \
                .options(header='true') \
                .options(delimiter=",") \
                .options(inferSchema='true') \
                .load(path)
            print("Done")

    def set_air_table_df(self, df):
        """This function set a dataframe into the class for subsequent actions.
        """
        assert isinstance(df, pyspark.sql.dataframe.DataFrame), "Error: df argument must a sql.dataframe type"
        self._air_table = df

    def get_air_table_df(self):
        """This function return the dataframe of the class"""
        return self._air_table

    @classmethod
    def extract_col_from_df(cls, df, column):
        """This function extract column from dataFrame.
        Returns a list of elements of column specified.

        :param df   Input dataframe
        :param column Column to extract from dataframe provided.
        :return list of elements of column extracted
        """
        return [x[0] for x in df.select(column).collect()]

    def read_col_names(self):
        """This function extract the column names from airtable dataset.

        :return list of tuples: [(oldNameCol1, newNameCol1),
                (oldNameCol2, newNameCol2)...]

                Where oldNameColX are the original names of columns in
                dataset, names are extracted from the airtable
                dataframe column 'Feature Name'.

                On the other hand, newNameColX are the new names of columns
                of dataset. New names are extracted from the airtable
                dataframe  'Code Name'
                                 """

        return list(zip(self.extract_col_from_df(self._air_table, 'Feature Name'),
                        self.extract_col_from_df(self._air_table, 'Code Name')))

    def read_new_col_names(self):
        """This function extract the new column names from airtable dataset.

        :return list of new column names. Those names are from column 'Code Name'
                                 """

        return self.extract_col_from_df(self._air_table, 'Code Name')

    def read_old_col_names(self):
        """This function extract the old column names from airtable dataset.

        :return list of old column names. Those names are from column 'Feature Name'
        """
        return self.extract_col_from_df(self._air_table, 'Feature Name')

    def read_col_types(self, names='newNames'):
        """This function extract the types of columns detailled in airtable dataset.

        :param  names   Name columns of dataFrame.  names variable consist of a list
                of columns read it from airtable dataFrame in the column specified.
                 names as argument can only have two values as input of this function:
                 'newNames' and 'oldNames. When 'newNames' is provided, the names are
                 read from 'Code Name' airtable dataFrame. On the other hand, when
                 'oldNames' is provided, 'Feature Name' is read it from airtable
                 dataFrame.


                 Column names are read using the function readNewColNames(self).
                 Types are extracted from 'DataType'

        :return list of colNames and their data types in the following form:
                [(colName1, 'string'), (colName2, 'integer'), (colName3, 'float')]
        """

        assert (names == 'newNames') or (names == 'oldNames'), "Error, names argument" \
                                                               "only can be the following values:" \
                                                               "'newNames' or 'oldNames'"

        col_names = {'newNames': 'Code Name', 'oldNames': 'Feature Name'}
        return list(zip(self.extract_col_from_df(self._air_table, col_names[names]),
                        self.extract_col_from_df(self._air_table, 'DataType')))
