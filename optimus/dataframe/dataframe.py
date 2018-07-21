from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from optimus.helpers.decorators import *
from optimus.helpers.functions import *
from pyspark.ml.feature import QuantileDiscretizer
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
