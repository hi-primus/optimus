from .optimus import *
# pyspark_pipes: build Spark ML pipelines easily
from .ml.pipelines import patch
patch()

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages databricks:spark-deep-learning:1.1.0-spark2.3-s_2.11 pyspark-shell'

