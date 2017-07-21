# -*- coding: utf-8 -*-
# Importing os module for system operative utilities
import os
# Importing SQLContext:
from pyspark.sql import SQLContext
# Importando modulo para eliminar carpetas
from shutil import rmtree
# Importando modulo para trabajar con urls
import urllib.request
# Importando modulo de expresiones regulares
import re

class Utilites():
    def __init__(self, sc):
        """

        :param SparkContext:
         A Spark context is essentially a client of Spark’s execution environment and acts as the master of your
         Spark application (don’t get confused with the other meaning of Master in Spark, though).
        """

        # Setting SQLContext as a global variable of the class
        self.sqlContext = SQLContext(sc)
        # Setting SparkContent as a global variable of the class
        self.__sc = sc

    def readDatasetCsv(self, path, delimiterMark=';', header='true'):
        """This funcion read a dataset from a csv file.

        :param path     Path or location of the file.
        :param delimiterMark   Usually delimiter mark are ',' or ';'.
        :param  header:     Tell the function is the dataset has a header row.

        :return dataFrame
        """
        assert ((header == 'true') or (header == 'false')), "Error, header argument must be 'true' or 'false'. " \
                                                             "header must be string dataType, i.e: header='true'"
        assert isinstance(delimiterMark, str), "Error, delimiter mark argumen must be string dataType."

        assert isinstance(path, str), "Error, path argument must be string datatype."

        return self.sqlContext \
                .read \
                .format('com.databricks.spark.csv') \
                .options(header=header) \
                .options(delimiter=delimiterMark) \
                .options(inferSchema='true') \
                .load(path)

    def readDatasetParquet(self, path):
        """This function allows user to read parquet files. It is import to clarify that this method is just based
        on the sqlContext.read.parquet(path) Apache Spark method. Only assertion instructions has been added to
        ensure user has more hints about what happened when something goes wrong.
        :param  path    Path or location of the file. Must be string dataType.

        :return dataFrame"""
        assert isinstance(path, str), "Error: path argument must be string dataType."
        assert (("file:///" == path[0:8]) or ("hdfs:///" == path[0:8])), "Error: path must be with a 'file://' prefix \
        if the file is in the local disk or a 'path://' prefix if the file is in the Hadood file system"
        return sqlContext.read.parquet(path)


    def csvToParquet(self, inputPath, outputPath, delimiterMarkCsv, headerCsv, numPartitions=None):
        """This method transform a csv dataset file into a parquet.

        The method reads a existing csv file using the inputPath, delimiterMarkCsv and headerCsv arguments.

        :param  inputPath   Address location of the csv file.
        :param  outputPath  Address where the new parquet file will be stored.
        :param  delimiterMarkCsv    Delimiter mark of the csv file, usually is ',' or ';'.
        :param  headerCsv   This argument specifies if csv file has header or not.
        :param  numParitions Specifies the number of partitions the user wants to write the dataset."""

        df = self.readDatasetCsv(inputPath, delimiterMarkCsv, 'true')

        if numPartitions != None:
            assert (numPartitions <= df.rdd.getNumPartitions()), "Error: numPartitions specified is greater that the" \
                                                                "partitions in file store in memory."
            # Writting dataset:
            df.coalesce(numPartitions).write.parquet(outputPath)

        else:
            df.write.parquet(outputPath)

    def setCheckPointFolder(self, path, fileSystem):
        """Function that receives a workspace path where a folder is created.
        This folder will store temporal
        dataframes when user writes the DataFrameTransformer.checkPoint().

        This function needs the sc parameter, which is the spark context in order to
        tell spark where is going to save the temporal files.

        It is recommended that users deleted this folder after all transformations are completed
        and the final dataframe have been saved. This can be done with deletedCheckPointFolder function.

        :param path     Location of the dataset (string).
        :param fileSystem   Describes if file system is local or hadoop file system.

        """

        assert (isinstance(fileSystem, str)), \
            "Error: fileSystem argument must be a string."

        assert (fileSystem == "hadoop") or (fileSystem == "local"), \
            "Error, fileSystem argument only can be 'local' or 'hadoop'"


        if fileSystem == "hadoop":
            folderPath = path + "/" + "checkPointFolder"
            self.deleteCheckPointFolder(path=path, fileSystem=fileSystem)

            # Creating file:
            print ("Creation of hadoop folder...")
            command = "hadoop fs -mkdir " + folderPath
            print ("$" + command)
            os.system(command)
            print ("Hadoop folder created. \n")

            print ("Setting created folder as checkpoint folder...")
            self.__sc.setCheckpointDir(folderPath)
            print ("Done.")


        else:
            # Folder path:
            folderPath = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            print ("Deleting previous folder if exists...")
            if os.path.isdir(folderPath):

                # Deletes folder if exits:
                rmtree(folderPath)
                print ("Creation of checkpoint directory...")
                # Creates new folder:
                os.mkdir(folderPath)
                print ("Done.")
            else:
                print ("Creation of checkpoint directory...")

                # Creates new folder:
                os.mkdir(folderPath)
                print ("Listo.")

            self.__sc.setCheckpointDir("file://" + folderPath)



    def deleteCheckPointFolder(self, path, fileSystem):
        """Function that deletes the temporal folder where temp files were stored.
        The path required is the same provided by user in setCheckPointFolder().

        :param fileSystem   Describes if file system is local or hadoop file system.
        """
        assert (isinstance(fileSystem, str)), "Error: fileSystem argument must be a string."

        assert (fileSystem == "hadoop") or (fileSystem == "local"), \
            "Error, fileSystem argument only can be 'local' or 'hadoop'"

        if fileSystem == "hadoop":
            # Folder path:
            folderPath = path + "/" + "checkPointFolder"
            print ("Deleting checkpoint folder...")
            command = "hadoop fs -rm -r " + folderPath
            os.system(command)
            print ("$" + command)
            print ("Folder deleted. \n")

        else:
            print ("Deleting checkpoint folder...")
            # Folder path:
            folderPath = path + "/" + "checkPointFolder"
            # Checking if tempFolder exits:
            if os.path.isdir(folderPath):
                # Deletes folder if exits:
                rmtree(folderPath)
                # Creates new folder:
                print ("Folder deleted. \n")
            else:
                print ("Folder deleted. \n")
                pass

    def getColumnNamesbyType(self, df, dataType):
        """This function returns column names of dataFrame which have the same
        datatype provided. It analyses column datatype by dataFrame.dtypes method.

        :return    List of column names of a type specified.
        """
        assert (dataType in ['string', 'integer', 'float', 'date', 'double']), \
            "Error, dataType only can be one of the following values: 'string', 'integer', 'float', 'date', 'double'"

        dicc = {'string': 'string','integer': 'int','float': 'float', 'double': 'double'}

        return list(y[0] for y in filter(lambda x: x[1] == dicc[dataType], df.dtypes))



class Airtable():
    def __init__(self, sc, path):
        # Setting airtable dataset variable
        self.__airtable = 0
        # Setting SQLContext as a global variable of the class
        self.sqlContext = SQLContext(sc)
        # Reading dataset
        self.readAirtableCsv(path)

    def readAirtableCsv(self, path):
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
            data = response.read()      # a `bytes` object
            # Decoding data:
            text = data.decode('utf-8')
            # Here text is splitted by ',' adn replace
            values =  [line.replace("\"", "").split(",") for line in text.split("\n")]

            # Airtable datafra that corresponds to airtable table:
            self.__airtable = sc.parallelize(values[1:]).toDF(values[0])
            print("Done")

        else:
            print("Reading local file...")
            self.__airtable = self.sqlContext.read \
            .format('com.databricks.spark.csv') \
            .options(header='true') \
            .options(delimiter=",") \
            .options(inferSchema='true') \
            .load(path)
            print("Done")


    def setAirtableDF(self, df):
        """This function set a dataframe into the class for subsequent actions.
        """
        assert isinstance(df, pyspark.sql.dataframe.DataFrame), "Error: df argument must a sql.dataframe type"
        self.__airtable = df

    def getAirtableDF(self):
        """This function return the dataframe of the class"""
        return self.__airtable

    def extractColFromDF(self, df, column):
        """This function extract column from dataFrame.
        Returns a list of elements of column specified.

        :param df   Input dataframe
        :param column Column to extract from dataframe provided.
        :return list of elements of column extracted
        """
        return [x[0] for x in df.select(column).collect()]

    def readColNames(self):
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

        return list(zip(self.extractColFromDF(self.__airtable, 'Feature Name'),
                        self.extractColFromDF(self.__airtable, 'Code Name')))

    def readNewColNames(self):
        """This function extract the new column names from airtable dataset.

        :return list of new column names. Those names are from column 'Code Name'
                                 """

        return self.extractColFromDF(self.__airtable, 'Code Name')

    def readOldColNames(self):
        """This function extract the old column names from airtable dataset.

        :return list of old column names. Those names are from column 'Feature Name'
        """
        return self.extractColFromDF(self.__airtable, 'Feature Name')

    def readColTypes(self, names='newNames'):
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

        colNames = {'newNames': 'Code Name', 'oldNames': 'Feature Name'}
        return list(zip(self.extractColFromDF(self.__airtable, colNames[names]),
                        self.extractColFromDF(self.__airtable, 'DataType')))

