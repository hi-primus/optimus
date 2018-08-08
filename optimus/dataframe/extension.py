import logging
import os

import jinja2
from IPython.core.display import display, HTML
from pyspark.ml.feature import SQLTransformer
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from optimus.helpers.decorators import *
from optimus.helpers.functions import parse_columns, collect_to_dict
from optimus.spark import Spark


@add_method(DataFrame)
def melt(self, df, id_vars, value_vars, var_name="variable", value_name="value"):
    """
    Convert DataFrame from wide to long format.
    :param self:
    :param df: Dataframe to be melted
    :param id_vars:
    :param value_vars:
    :param var_name:
    :param value_name:
    :return:
    """

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = [*(
        F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
        for c in value_vars)]

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", F.explode(F.array(_vars_and_vals)))

    cols = id_vars + [
        F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


@add_method(DataFrame)
def size(self):
    """
    Get the size of a dataframe in bytes
    :param self:
    :return:
    """

    def _to_java_object_rdd(rdd):
        """
        Return a JavaRDD of Object by unpickling
        It will convert each Python object into Java object by Pyrolite, whenever the
        RDD is serialized in batch or not.
        """
        rdd = rdd._reserialize(AutoBatchedSerializer(PickleSerializer()))
        return rdd.ctx._jvm.org.apache.spark.mllib.api.python.SerDe.pythonToJava(rdd._jrdd, True)

    java_obj = _to_java_object_rdd(self.rdd)

    n_bytes = Spark.instance.sc()._jvm.org.apache.spark.util.SizeEstimator.estimate(java_obj)
    return n_bytes


@add_attr(DataFrame)
def run(self):
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

    logging.info("Saving changes at disk by checkpoint...")

    self.cache().count

    logging.info("Done.")

    return True


@add_attr(DataFrame)
def sql(self, sql_expression):
    """
    Implements the transformations which are defined by SQL statement. Currently we only support
    SQL syntax like "SELECT ... FROM __THIS__ ..." where "__THIS__" represents the
    underlying table of the input dataframe.
    :param self:
    :param sql_expression: SQL expression.
    :return: Dataframe with columns changed by SQL statement.
    """

    self._assert_type_str(sql_expression, "sql_expression")

    sql_transformer = SQLTransformer(statement=sql_expression)

    self._df = sql_transformer.transform(self)

    return self


@add_attr(DataFrame)
def table(self, columns=None, limit=100):
    """
    Return a HTML table with the dataframe cols, data types and values
    :param self
    :param columns:
    :param limit:
    :return:
    """

    if columns is None:
        columns = "*"
    columns = parse_columns(self, columns)

    data = collect_to_dict(self.select(columns).limit(limit).collect())

    path = os.path.dirname(os.path.abspath(__file__))

    template_loader = jinja2.FileSystemLoader(searchpath=path + "//../templates")
    template_env = jinja2.Environment(loader=template_loader, autoescape=True)

    template = template_env.get_template("table.html")

    # Filter only the columns and data type need it
    dtypes = list(filter(lambda x: x[0] in columns, self.dtypes))

    output = template.render(cols=dtypes, data=data)
    display(HTML(output))
