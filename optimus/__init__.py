# pyspark_pipes: build Spark ML pipelines easily
from .ml.pipelines import patch
from .optimus import *

patch()

# Monkey patch for Spark DataFrames
from optimus.dataframe import rows, columns, extension, plots
from optimus.io import save
