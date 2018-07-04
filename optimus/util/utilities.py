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


