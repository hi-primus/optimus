# Importing DataFrameAnalyzer library
from optimus.df_analyzer import DataFrameAnalyzer

# Importing Utilities
from optimus.utilities import *

# Importing Outliers library
from optimus.df_outliers import *

# Importing Outliers library
from optimus.optimus import *


# Importing Spark session
from pyspark.sql.session import SparkSession

# Importing iPython
from IPython.display import display, HTML

# Basic imports
import os


# -*- coding: utf-8 -*-

# pyspark_pipes: build Spark ML pipelines easily

from .ml import patch
from .display_pipes import print_stage

from .assertion_helpers import *

sc = None
spark = None

# Messages strings
STARTING = "Starting or getting SparkSession and SparkContext..."
CHECK_POINT_CONFIG = "Setting checkpoint folder (local). If you are in a cluster change it with set_check_point_ " \
              "folder(path,'hadoop')."
SUCCESS = "Optimus successfully imported. Have fun :)."

def init_spark():
    """
        Initialize Spark
        :return:
    """
    print("Just checking that all necessary environments vars are present...")
    print("PYSPARK_PYTHON=" + os.environ.get("PYSPARK_PYTHON"))
    print("SPARK_HOME=" + os.environ.get("SPARK_HOME"))
    print("JAVA_HOME=" + os.environ.get("JAVA_HOME"))
    print("-----")
    print(STARTING)

    global spark
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    global sc
    sc = spark.sparkContext

    print(CHECK_POINT_CONFIG)

    Utilities().set_check_point_folder(os.getcwd(), "local")


def print_html(html):
    """
    Just a display() helper to print html code
    :param html: html code to be printed
    :return:
    """
    display(HTML(html))

patch()

try:
    get_ipython

    init_spark()

    print_html(
        """
        <div style="margin:10px">
            <a href="https://github.com/ironmussa/Optimus" target="_new">
                <img src="https://github.com/ironmussa/Optimus/raw/master/images/robotOptimus.png" style="float:left;margin-right:10px;vertical-align:top;text-align:center" height="50" width="50"/>
            </a>
            <span>{0}</span>
        </div>
        """.format("<b><h2>" + SUCCESS + "</h2></b>")
    )
except Exception:
    print("Shell detected")

    init_spark()

    print(SUCCESS)
    print("""
   ____        __  _                     
  / __ \____  / /_(_)___ ___  __  _______
 / / / / __ \/ __/ / __ `__ \/ / / / ___/
/ /_/ / /_/ / /_/ / / / / / / /_/ (__  ) 
\____/ .___/\__/_/_/ /_/ /_/\__,_/____/  
    /_/                                  
    """)

# module level doc-string
__doc__ = """
Optimus = Optimus is the missing framework for cleansing (cleaning and much more), pre-processing and exploratory data 
analysis in a distributed fashion with Apache Spark.
=====================================================================
Optimus the missing framework for cleansing (cleaning and much more), pre-processing and exploratory data analysis in a 
distributed fashion. It uses all the power of Apache Spark to do so. It implements several handy tools for data 
wrangling and munging that will make your life much easier. The first obvious advantage over any other public data 
cleaning library is that it will work on your laptop or your big cluster, and second, it is amazingly easy to 
install, use and understand.
"""
