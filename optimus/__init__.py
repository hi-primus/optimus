# pyspark_pipes: build Spark ML pipelines easily
import timber

from .ml.pipelines import patch
from .optimus import *

patch()

import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages databricks:spark-deep-learning:1.1.0-spark2.3-s_2.11 pyspark-shell'

# Monkey patch for Spark DataFrames
from optimus.dataframe import rows, columns, extension, plots
from optimus.io import save

