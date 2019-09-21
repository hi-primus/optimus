import os
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # This is your Project Root

# pyspark_pipes: build Spark ML pipelines easily
from .ml.pipelines import patch
from .optimus import *

patch()

# Monkey patch for Spark DataFrames
from .dataframe import rows, columns, extension
from .dataframe.plots import plots
from .io import save
# from optimus.outliers import outliers
# from optimus.profiler.profiler import Profiler
# from optimus.bumblebee import Comm


# Handle encoding problem
# https://stackoverflow.com/questions/39662384/pyspark-unicodeencodeerror-ascii-codec-cant-encode-character

os.environ["PYTHONIOENCODING"] = "utf8"
