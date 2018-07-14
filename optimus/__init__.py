from .optimus import *

from optimus.spark import Spark
# We use this to save a reference to the Spark session at the module level
Spark.instance = None
