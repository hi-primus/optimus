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


    @classmethod
    def get_column_names_by_type(cls, df, data_type):
        """
        This function returns column names of dataFrame which have the same
        datatype provided. It analyses column datatype by dataFrame.dtypes method.

        :return    List of column names of a type specified.

        :param df:
        :param data_type:
        :return:
        """
        assert (data_type in ['string', 'integer', 'float', 'date', 'double']), \
            "Error, data_type only can be one of the following values: 'string', 'integer', 'float', 'date', 'double'"

        dicc = {'string': 'string', 'integer': 'int', 'float': 'float', 'double': 'double'}

        return list(y[0] for y in filter(lambda x: x[1] == dicc[data_type], df.dtypes))


    def age_calculate(self, column, dates_format, name_col_age):
        """
        This method compute the age of clients based on their born dates.
        :param  column      Name of the column born dates column.
        :param  dates_format  String format date of the column provided.
        :param  name_col_age  Name of the new column, the new columns is the resulting column of ages.

        """
        # Check if column argument a string datatype:
        column = self._assert_columns_names(column)
        assert len(column) == 1, "Error: Columns must be a string or a list of one element"

        # Check if dates_format argument a string datatype:
        self._assert_type_str(dates_format, "dates_format")

        # Asserting if column if in dataFrame:
        name_col_age = self._assert_columns_names(name_col_age)
        assert len(name_col_age) == 1, "Error: Columns must be a string or a list of one element"

        # Output format date
        format_dt = "yyyy-MM-dd"  # Some SimpleDateFormat string

        exprs = format_number(
            mag(
                months_between(date_format(
                    unix_timestamp(column, dates_format).cast("timestamp"), format_dt), current_date()) / 12), 4).alias(
            name_col_age)

        return self.withColumn(name_col_age, exprs)

    def date_transform(self, columns, current_format, output_format):
        """
        :param  columns     Name date columns to be transformed. Columns ha
        :param  current_format   current_format is the current string dat format of columns specified. Of course,
                                all columns specified must have the same format. Otherwise the function is going
                                to return tons of null values because the transformations in the columns with
                                different formats will fail.
        :param  output_format    output date string format to be expected.
        """
        # Check if current_format argument a string datatype:
        self._assert_type_str(current_format, "current_format")
        # Check if output_format argument a string datatype:
        self._assert_type_str(output_format, "output_format")
        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        if isinstance(columns, str):
            columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        exprs = [date_format(unix_timestamp(c, current_format).cast("timestamp"), output_format).alias(
            c) if c in columns else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        self._add_transformation()  # checkpoint in case

        return self

    def check_point(self):
        """
        This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
        evaluation approach in processing data: transformation functions are not computed into an action is called.
        Sometimes when transformations are numerous, the computations are very extensive because the high number of
        operations that spark needs to run in order to get the results.

        Other important thing is that apache spark usually save task but not result of dataFrame, so tasks are
        accumulated and the same situation happens.

        The problem can be deal it with the checkPoint method. This method save the resulting dataFrame in disk, so
         the lineage is cut.
        """

        # Checkpointing of dataFrame. One question can be thought. Why not use cache() or persist() instead of
        # checkpoint. This is because cache() and persis() apparently do not break the lineage of operations,
        print("Saving changes at disk by checkpoint...")
        self.checkpoint()
        self.count()
        self = self._sql_context.createDataFrame(self, self.schema)
        print("Done.")

    execute = check_point

    @classmethod
    def delete_check_point_folder(cls, path, file_system):
        """
        Function that deletes the temporal folder where temp files were stored.
        The path required is the same provided by user in setCheckPointFolder().

        :param path:
        :param file_system: Describes if file system is local or hadoop file system.
        :return:
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

    def set_check_point_folder(self, path, file_system):
        """
        Function that receives a workspace path where a folder is created.
        This folder will store temporal
        dataframes when user writes the DataFrameTransformer.checkPoint().

        This function needs the sc parameter, which is the spark context in order to
        tell spark where is going to save the temporal files.

        It is recommended that users deleted this folder after all transformations are completed
        and the final dataframe have been saved. This can be done with deletedCheckPointFolder function.

        :param path: Location of the dataset (string).
        :param file_system: Describes if file system is local or hadoop file system.

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

    def clear_accents(self, columns):
        """This function deletes accents in strings column dataFrames, it does not eliminate main characters,
        but only deletes special tildes.

        :param columns  String or a list of column names.

        """

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
                col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
                                          % col_not_valids

        # Receives  a string as an argument
        def remove_accents(input_str):
            # first, normalize strings:
            nfkd_str = unicodedata.normalize('NFKD', input_str)
            # Keep chars that has no other char combined (i.e. accents chars)
            with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
            return with_out_accents

        function = udf(lambda x: remove_accents(x) if x is not None else x, StringType())
        exprs = [function(col(c)).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]
        self._df = self._df.select(*exprs)

        self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def remove_special_chars(self, columns):
        """This function remove special chars in string columns, such as: .!"#$%&/()
        :param columns      list of names columns to be processed.

        columns argument can be a string or a list of strings."""

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str):
            columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
                col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
                                          % col_not_valids

        def rm_spec_chars(input_str):
            # Remove all punctuation and control characters
            for punct in (set(input_str) & set(string.punctuation)):
                input_str = input_str.replace(punct, "")
            return input_str

        # User define function that does operation in cells
        function = udf(lambda cell: rm_spec_chars(cell) if cell is not None else cell, StringType())

        exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def remove_special_chars_regex(self, columns, regex):
        """This function remove special chars in string columns using a regex, such as: .!"#$%&/()
        :param columns      list of names columns to be processed.
        :param regex        string that contains the regular expression

        columns argument can be a string or a list of strings."""

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str):
            columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
                col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
                                          % col_not_valids

        def rm_spec_chars_regex(input_str, regex):
            for _ in set(input_str):
                input_str = re.sub(regex, '', input_str)
            return input_str

        # User define function that does operation in cells
        function = udf(lambda cell: rm_spec_chars_regex(cell, regex) if cell is not None else cell, StringType())

        exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self