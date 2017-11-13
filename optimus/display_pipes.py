# -*- coding: utf-8 -*-
"""
Some pipeline display functionality
"""

from pyspark.ml import Pipeline
from pyspark.ml.param.shared import HasFeaturesCol, HasInputCol, \
    HasInputCols, HasPredictionCol, HasOutputCol


def print_stage(pipe):
    """
    Print stage of a pipeline even recursively
    :param pipe: Pipeline
    :return: str
    """
    if isinstance(pipe, Pipeline):
        return "[\n" + ','.join([print_stage(s) for s in pipe.getStages()]) + "\n]"
    else:
        result = ""
        if isinstance(pipe, HasInputCol):
            result += pipe.getInputCol()
        elif isinstance(pipe, HasInputCols):
            result += str(pipe.getInputCols())
        elif isinstance(pipe, HasFeaturesCol):
            result += pipe.getFeaturesCol()

        result += " - "
        if isinstance(pipe, HasOutputCol):
            result += pipe.getOutputCol()
        elif isinstance(pipe, HasPredictionCol):
            result += pipe.getPredictionCol()
        return result
