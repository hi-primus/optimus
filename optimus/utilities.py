# -*- coding: utf-8 -*-
# Importing os module for system operative utilities
import os
# Importing SQLContext:
from pyspark.sql.session import SparkSession
# Importando modulo para eliminar carpetas
from shutil import rmtree
# Importando modulo para trabajar con urls
import urllib.request
# Importando modulo de expresiones regulares
import re
# Import SparkContext
import pyspark


class Utilites:
    def __init__(self):

        # Setting SQLContext as a global variable of the class
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        # Setting SparkContent as a global variable of the class
        self.__sc = self.spark.sparkContext

    def read_dataset_csv(self, path, delimiter_mark=';', header='true'):
        """This funcion read a dataset from a csv file.

        :param path     Path or location of the file.
        :param delimiter_mark   Usually delimiter mark are ',' or ';'.
        :param  header:     Tell the function is the dataset has a header row.

        :return dataFrame
        """
        assert ((header == 'true') or (header == 'false')), "Error, header argument must be 'true' or 'false'. " \
                                                            "header must be string dataType, i.e: header='true'"
        assert isinstance(delimiter_mark, str), "Error, delimiter mark argumen must be string dataType."

        assert isinstance(path, str), "Error, path argument must be string datatype."

        return self.spark.read \
            .format('csv') \
            .options(header=header) \
            .options(delimiter=delimiter_mark) \
            .options(inferSchema='true') \
            .load(path)

    def read_dataset_parquet(self, path):
        """This function allows user to read parquet files. It is import to clarify that this method is just based
        on the sqlContext.read.parquet(path) Apache Spark method. Only assertion instructions has been added to
        ensure user has more hints about what happened when something goes wrong.
        :param  path    Path or location of the file. Must be string dataType.

        :return dataFrame"""
        assert isinstance(path, str), "Error: path argument must be string dataType."
        assert (("file:///" == path[0:8]) or ("hdfs:///" == path[0:8])), "Error: path must be with a 'file://' prefix \
        if the file is in the local disk or a 'path://' prefix if the file is in the Hadood file system"
        return self.spark.read.parquet(path)

    def csv_to_parquet(self, input_path, output_path, delimiter_mark_csv, header_csv, num_partitions=None):
        """This method transform a csv dataset file into a parquet.

        The method reads a existing csv file using the inputPath, delimiter_mark_csv and headerCsv arguments.

        :param  input_path   Address location of the csv file.
        :param  output_path  Address where the new parquet file will be stored.
        :param  delimiter_mark_csv    Delimiter mark of the csv file, usually is ',' or ';'.
        :param  header_csv   This argument specifies if csv file has header or not.
        :param  num_partitions Specifies the number of partitions the user wants to write the dataset."""

        df = self.read_dataset_csv(input_path, delimiter_mark_csv, header=header_csv)

        if num_partitions is not None:
            assert (num_partitions <= df.rdd.getNumPartitions()), "Error: num_partitions specified is greater that the" \
                                                                  "partitions in file store in memory."
            # Writting dataset:
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
                print("Listo.")

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


class Airtable:
    def __init__(self, path):
        # Setting airtable dataset variable
        self._air_table = None
        # Setting SQLContext as a global variable of the class
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
