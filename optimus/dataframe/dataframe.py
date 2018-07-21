from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.ml.feature import QuantileDiscretizer

from optimus.helpers.decorators import *
from optimus.helpers.functions import *
from optimus.spark import Spark

import pandas as pd


@add_method(DataFrame)
def melt(self, id_vars, value_vars, var_name="variable", value_name="value"):
    """

    :param self:
    :param id_vars:
    :param value_vars:
    :param var_name:
    :param value_name:
    :return:
    """
    _vars_and_vals = array(*(F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name)) for c in value_vars))

    # Add to the DataFrame and explode
    tmp = self.withColumn("_vars_and_vals", F.explode(_vars_and_vals))
    cols = id_vars + [F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]

    return tmp.select(*cols)


@add_method(DataFrame)
def _hist(self, column, bins=10):
    """

    :param self:
    :param column:
    :param bins:
    :return:
    """

    temp_col = "bucket"
    discretizer = QuantileDiscretizer(numBuckets=10, inputCol=column, outputCol=temp_col)
    df = discretizer.fit(self).transform(self)
    return collect_to_dict(df.groupBy(temp_col).agg(F.min(column).alias('min'), F.max(column).alias('max'),
                                                    F.count(temp_col).alias('count')).orderBy(temp_col).collect())


@add_method(DataFrame)
def hist(self, columns, bins=10):
    """

    :param self:
    :param columns:
    :param bins:
    :return:
    """
    columns = parse_columns(self, columns)
    return format_dict({c: self._hist(c, bins) for c in columns})


@staticmethod
@add_method(DataFrame)
def plot(columns):
    """
    :param columns:
    :return:
    """

    histogram_result = _hist(columns, 10)

    pd.DataFrame(
        list(zip(*histogram_result)),
        columns=['bin', 'frequency']
    ).set_index(
        'bin'
    ).plot(kind='bar');


@add_method(DataFrame)
def size(self):
    """
    Get the size of a dataframe in bytes
    :param self:
    :return:
    """
    def _to_java_object_rdd(rdd):
        """ Return a JavaRDD of Object by unpickling
        It will convert each Python object into Java object by Pyrolite, whenever the
        RDD is serialized in batch or not.
        """
        rdd = rdd._reserialize(AutoBatchedSerializer(PickleSerializer()))
        return rdd.ctx._jvm.org.apache.spark.mllib.api.python.SerDe.pythonToJava(rdd._jrdd, True)

    java_obj = _to_java_object_rdd(self.rdd)

    nbytes = Spark.get_sc()._jvm.org.apache.spark.util.SizeEstimator.estimate(java_obj)
    return nbytes


@add_attr(DataFrame)
def execute(self):
    """
    This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
    evaluation approach in processing data: transformation functions are not computed into an action is called.
    Sometimes when transformations are numerous, the computations are very extensive because the high number of
    operations that spark needs to run in order to get the results.

    Other important thing is that apache spark usually save task but not result of dataFrame, so tasks are
    accumulated and the same situation happens.

    The problem can be deal it with the checkPoint method. This method save the resulting dataFrame in disk, so
     the lineage is cut.
    """

    # Checkpointing of dataFrame. One question can be thought. Why not use cache() or persist() instead of
    # checkpoint. This is because cache() and persis() apparently do not break the lineage of operations,
    print("Saving changes at disk by checkpoint...")
    df = self

    df.checkpoint()
    df.count()
    # self = self._sql_context.createDataFrame(self, self.schema)
    print("Done.")

    return None
