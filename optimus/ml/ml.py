"""
Code based on pyspark_pipes by Daniel Acuña
"""

import pyspark

from pyspark.ml.param.shared import HasFeaturesCol, HasInputCol, \
    HasInputCols, HasLabelCol, HasPredictionCol, HasOutputCol, Params, \
    HasRawPredictionCol, HasProbabilityCol

from pyspark.ml import feature, classification
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, GBTClassifier
from pyspark.ml import Pipeline

import optimus as op

ALLOWED_TYPES = (HasFeaturesCol, HasInputCol, HasInputCols, HasLabelCol,
                 HasPredictionCol, HasOutputCol)


def assert_spark_df(df):
    assert (isinstance(df, pyspark.sql.dataframe.DataFrame))






