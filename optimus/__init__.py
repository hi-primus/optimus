# Importing DataFrameTransformer library
from optimus.df_transformer import DataFrameTransformer
# Importing DataFrameAnalyzer library
from optimus.df_analyzer import DataFrameAnalyzer
# Importing DfProfiler library
from optimus.df_analyzer import DataFrameProfiler
# Importing Utility library
from optimus.utilities import *
# Importing Outliers library
from optimus.df_outliers import *
# Importing iPython
from IPython.display import display, HTML
# Importing Spark session
from pyspark.sql.session import SparkSession
# Basic imports
import os

# -*- coding: utf-8 -*-

# pyspark_pipes: build Spark ML pipelines easily

from .ml import patch
from .display_pipes import print_stage

patch()

try:
    get_ipython

    def print_html(html):
        display(HTML(html))

    print_html("<div>Starting or getting SparkSession and SparkContext.</div>")

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sc = spark.sparkContext

    print_html("<div>Setting checkpoint folder (local). If you are in a cluster change it with "
               "set_check_point_folder(path,'hadoop').</div>")

    Utilities().set_check_point_folder(os.getcwd(), "local")

    message = "<b><h2>Optimus successfully imported. Have fun :).</h2></b>"

    print_html(
        """
        <div style="margin:10px">
            <a href="https://github.com/ironmussa/Optimus" target="_new">
                <img src="https://github.com/ironmussa/Optimus/raw/master/images/robotOptimus.png" style="float:left;margin-right:10px;vertical-align:top;text-align:center" height="50" width="50"/>
            </a>
            <span>{0}</span>
        </div>
        """.format(message)
    )
except Exception:
    print("Shell detected")
    print("Starting or getting SparkSession and SparkContext.")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    print("Setting checkpoint folder (local). If you are in a cluster change it with set_check_point_"
          "folder(path,'hadoop').")
    Utilities().set_check_point_folder(os.getcwd(), "local")
    print("SparkSession and Context initialized. CheckPoint folder created. Optimus successfully imported. Have fun :).")
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
