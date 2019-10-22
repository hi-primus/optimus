import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # This is your Project Root

# SPARK MONKEY PATCH
# pyspark_pipes: build Spark ML pipelines easily
from optimus.spark.ml.pipelines import patch
from .optimus import *

patch()

from .spark import rows, columns, extension, constants
from .spark.plots import plots
from optimus.spark.io import save

# Handle encoding problem
# https://stackoverflow.com/questions/39662384/pyspark-unicodeencodeerror-ascii-codec-cant-encode-character

os.environ["PYTHONIOENCODING"] = "utf8"

# PANDAS MONKEY PATCH
from .pandas import columns, functions

# DASK MONKEY PATCH
from .dask import columns, rows, constants, extension, functions
