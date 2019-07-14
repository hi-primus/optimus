import os
ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) # This is your Project Root

# pyspark_pipes: build Spark ML pipelines easily
from .ml.pipelines import patch
from .optimus import *

patch()

# Monkey patch for Spark DataFrames
from optimus.dataframe import rows, columns, extension
from optimus.dataframe.plots import plots
from optimus.outliers import outliers
from optimus.io import save
