"""
Code based on pyspark_pipes by Daniel Acuña
"""
from pyspark.ml import Pipeline
from pyspark.ml.param.shared import HasFeaturesCol, HasInputCol, \
    HasInputCols, HasPredictionCol, HasOutputCol, HasLabelCol, Params, \
    HasRawPredictionCol, HasProbabilityCol

ALLOWED_TYPES = (HasFeaturesCol, HasInputCol, HasInputCols, HasLabelCol,
                 HasPredictionCol, HasOutputCol)


def set_default_colnames(pipe_stage):
    """
    Add object ids to default columns
    :param pipe_stage: Params object
    :return: modified stage
    """
    # pylint: disable=protected-access
    if isinstance(pipe_stage, HasFeaturesCol) and not pipe_stage.isSet('featuresCol'):
        pipe_stage._setDefault(featuresCol=pipe_stage.uid + '__features')
    if isinstance(pipe_stage, HasRawPredictionCol) and not pipe_stage.isSet('rawPredictionCol'):
        pipe_stage._setDefault(rawPredictionCol=pipe_stage.uid + '__rawPrediction')
    if isinstance(pipe_stage, HasProbabilityCol) and not pipe_stage.isSet('probabilityCol'):
        pipe_stage._setDefault(probabilityCol=pipe_stage.uid + '__probability')
    if isinstance(pipe_stage, HasPredictionCol) and not pipe_stage.isSet('predictionCol'):
        pipe_stage._setDefault(predictionCol=pipe_stage.uid + '__prediction')
    return pipe_stage


def is_instance(obj, typelist):
    """
    Returns true if object `obj` is of any of the types list in `typelist`
    :param obj: object
    :param typelist: list of types
    :return: bool
    """
    return any([isinstance(obj, t) for t in typelist])


def get_pipeline_laststep(pipe):
    """
    Get last stage of pipe that is not a Pipeline
    :param pipe: Params
    :return: Params
    """
    if isinstance(pipe, Pipeline):
        return get_pipeline_laststep(pipe.getStages()[-1])
    return pipe


def get_pipeline_firststep(pipe):
    """
    Get last stage of a pipe that is not a Pipeline
    :param pipe: Params
    :return: Estimator or Transformer
    """
    if isinstance(pipe, Pipeline):
        return get_pipeline_firststep(pipe.getStages()[0])

    return pipe


def left_pipe_function(self, other):
    """
    Infix pipe operator
    :param self: right-hand side object
    :param other: left-hand side object
    :return: Pipeline
    """
    # pylint: disable=too-many-branches

    self = set_default_colnames(self)
    # At the start of the Pipeline we need to put together two things
    if is_instance(other, ALLOWED_TYPES):
        result = Pipeline().setStages([other]) | self
    elif isinstance(other, Pipeline):
        last_step = set_default_colnames(get_pipeline_laststep(other))
        if isinstance(last_step, HasOutputCol):
            # let's generate some random string to represent the column's name
            last_step_output = last_step.getOutputCol()
        elif isinstance(last_step, HasPredictionCol):
            last_step_output = last_step.getPredictionCol()
        else:
            raise Exception("Type of step not supported")

        # should we connect input with output?
        first_step = set_default_colnames(get_pipeline_firststep(self))
        if isinstance(first_step, HasInputCol):
            if not first_step.isSet('inputCol'):
                first_step.setInputCol(last_step_output)
        if isinstance(first_step, HasFeaturesCol):
            if not first_step.isSet('featuresCol'):
                first_step.setFeaturesCol(last_step_output)

        result = Pipeline().setStages(other.getStages() + [self])
    elif isinstance(other, (tuple, list)):
        # check that connecting to one estimator or transformer
        if not isinstance(self, HasInputCols):
            raise Exception("When many to one connection, then receiver "
                            "must accept multiple inputs")

        all_outputs = []
        all_objects = []
        for stage in other:
            if not isinstance(stage, NotBroadcasted):
                last_step = set_default_colnames(get_pipeline_laststep(stage))

                if isinstance(last_step, HasOutputCol):
                    # let's generate some random string to represent the column's name
                    last_step_output = last_step.getOutputCol()
                elif isinstance(last_step, HasPredictionCol):
                    last_step_output = last_step.getPredictionCol()
                else:
                    raise Exception("It must contain output or prediction")

                all_outputs.append(last_step_output)
                all_objects.append(stage)
            else:
                all_objects.append(stage.object)

        # should we connect input with output?
        first_step = set_default_colnames(get_pipeline_firststep(self))
        if not first_step.isSet('inputCols'):
            first_step.setInputCols(all_outputs)

        result = Pipeline().setStages(all_objects + [self])
    else:
        raise Exception("Type of pipeline not supported")

    return result


def right_pipe_function(self, other):
    """
    Prefix pipe operator
    :param self: left-hand side operator
    :param other: right-hand side operator
    :return: Pipeline
    """
    self = set_default_colnames(self)
    if isinstance(other, (list, tuple)) and \
            isinstance(self, tuple(list(ALLOWED_TYPES) + [Pipeline])):
        last_step = set_default_colnames(get_pipeline_laststep(self))

        if isinstance(last_step, HasOutputCol):
            # let's generate some random string to represent the column's name
            last_step_output = last_step.getOutputCol()
        elif isinstance(last_step, HasPredictionCol):
            last_step_output = last_step.getPredictionCol()
        else:
            raise Exception("It must contain output or prediction")

        for stage in other:
            first_step = set_default_colnames(get_pipeline_firststep(stage))

            if isinstance(first_step, HasInputCol):
                # let's generate some random string to represent the column's name
                if not first_step.isSet('inputCol'):
                    first_step.setInputCol(last_step_output)
            elif isinstance(first_step, HasFeaturesCol):
                if not first_step.isSet('featuresCol'):
                    first_step.setFeaturesCol(last_step_output)
            else:
                raise Exception("A step didn't allow inputs")

        result = [NotBroadcasted(self)] + list(other)
    else:
        result = left_pipe_function(other, self)
    return result


class NotBroadcasted:
    """
    Wraps a Params object so that it doesn't broadcast it as part of a many to one
    pipe transformation
    """

    # pylint: disable=too-few-public-methods
    def __init__(self, params_object):
        self.object = params_object


def patch():
    """
    Monkey patches the Params class
    :return: None
    """
    Params.__or__ = right_pipe_function
    Params.__ror__ = left_pipe_function


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
