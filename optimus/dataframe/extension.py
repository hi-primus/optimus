
import logging
import math
import os

import humanize
import jinja2
from IPython.core.display import display, HTML
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.stat import Correlation
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from optimus.helpers.decorators import *
from optimus.helpers.functions import parse_columns, collect_as_dict, random_int, val_to_list, traverse
from optimus.spark import Spark


@add_method(DataFrame)
def roll_out():
    """
    Just a function to check if the Spark dataframe has been Monkey Patched
    :return:
    """
    print("Yes!")


@add_method(DataFrame)
def to_json(self):
    """
    Return a json from a Spark Dataframe
    :param self:
    :return:
    """
    return collect_as_dict(self.collect())


@add_method(DataFrame)
def export(self):
    """
    Helper function to export all the dataframe in text format. Aimed to be used in test functions
    :param self:
    :return:
    """
    dict_result = {}
    value = self.collect()
    schema = []
    for col_names in self.cols.names():
        name = col_names

        data_type = self.cols.schema_dtype(col_names)
        if isinstance(data_type, ArrayType):
            data_type = "ArrayType(" + str(data_type.elementType) + "()," + str(data_type.containsNull) + ")"
        else:
            data_type = str(data_type) + "()"

        nullable = self.schema[col_names].nullable

        schema.append("('{name}', {dataType}, {nullable})".format(name=name, dataType=data_type, nullable=nullable))
    schema = ",".join(schema)
    schema = "[" + schema + "]"

    # if there is only an element in the dict just return the value
    if len(dict_result) == 1:
        dict_result = next(iter(dict_result.values()))
    else:
        dict_result = [tuple(v.asDict().values()) for v in value]

    def func(path, _value):
        try:
            if math.isnan(_value):
                r = None
            else:
                r = _value
        except TypeError:
            r = _value
        return r

    dict_result = traverse(dict_result, None, func)

    return "{schema}, {dict_result}".format(schema=schema, dict_result=dict_result)


@add_method(DataFrame)
def sample_n(self, n=10, random=False):
    """
    Return a n number of sample from a dataFrame
    :param self:
    :param n: Number of samples
    :param random: if true get a semi random sample
    :return:
    """
    if random is True:
        seed = random_int()
    elif random is False:
        seed = 0

    rows_count = self.count()
    if n < rows_count:
        fraction = n / rows_count
    else:
        fraction = 1.0

    return self.sample(False, fraction, seed=seed)


@add_method(DataFrame)
def pivot(self, index, column, values):
    """
    Return reshaped DataFrame organized by given index / column values.
    :param self: Spark Dataframe
    :param index: Column to use to make new frame's index.
    :param column: Column to use to make new frame's columns.
    :param values: Column(s) to use for populating new frame's values.
    :return:
    """
    return self.groupby(index).pivot(column).agg(F.first(values))


@add_method(DataFrame)
def melt(self, id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
    """
    Convert DataFrame from wide to long format.
    :param self: Spark Dataframe
    :param id_vars: column with unique values
    :param value_vars: Column names that are going to be converted to columns values
    :param var_name: Column name for vars
    :param value_name: Column name for values
    :param data_type: All columns must have the same type. It will transform all collumns to this datatype.
    :return:
    """

    df = self
    id_vars = val_to_list(id_vars)
    # Cast all colums to the same type
    df = df.cols.cast(id_vars + value_vars, data_type)

    vars_and_vals = [F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name)) for c in value_vars]

    # Add to the DataFrame and explode
    df = df.withColumn("vars_and_vals", F.explode(F.array(*vars_and_vals)))

    cols = id_vars + [F.col("vars_and_vals")[x].alias(x) for x in [var_name, value_name]]

    return df.select(*cols)


@add_method(DataFrame)
def size(self):
    """
    Get the size of a dataframe in bytes
    :param self: Spark Dataframe
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

    n_bytes = Spark.instance.sc._jvm.org.apache.spark.util.SizeEstimator.estimate(java_obj)
    return n_bytes


@add_method(DataFrame)
def run(self):
    """
    This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
    evaluation approach in processing data: transformation functions are not computed into an action is called.
    Sometimes when transformations are numerous, the computations are very extensive because the high number of
    operations that spark needs to run in order to get the results.

    Other important thing is that Apache Spark save task but not result of dataFrame, so tasks are
    accumulated and the same situation happens.

    :param self: Spark Dataframe
    :return:
    """

    self.cache().count()
    return self


@add_method(DataFrame)
def sql(self, sql_expression):
    """
    Implements the transformations which are defined by SQL statement. Currently we only support
    SQL syntax like "SELECT ... FROM __THIS__ ..." where "__THIS__" represents the
    underlying table of the input dataframe.
    :param self: Spark Dataframe
    :param sql_expression: SQL expression.
    :return: Dataframe with columns changed by SQL statement.
    """

    sql_transformer = SQLTransformer(statement=sql_expression)

    return sql_transformer.transform(self)


@add_attr(DataFrame)
def partitions(self):
    """
    Return the dataframe partitions number
    :param self: Spark Dataframe
    :return: Number of partitions
    """
    return self.rdd.getNumPartitions()


@add_attr(DataFrame)
def partitioner(self):
    """
    Return the algorithm used to partition the dataframe
    :param self: Spark Dataframe
    :return:
    """
    return self.rdd.partitioner


@add_attr(DataFrame)
def glom(self):
    """

    :param self: Spark Dataframe
    :return:
    """
    return collect_as_dict(self.rdd.glom().collect()[0])


@add_method(DataFrame)
def h_repartition(self, partitions_number=None, col_name=None):
    """
    Apply a repartition to a datataframe based in some heuristics. Also you can pass the number of partitions and a column if you need more control.
    See https://stackoverflow.com/questions/35800795/number-of-partitions-in-rdd-and-performance-in-spark/35804407#35804407
    :param self: Spark Dataframe
    :param partitions_number: Number of partitions
    :param col_name: Column to be used to apply the repartition id necessary
    :return:
    """
    if partitions_number is None:
        partitions_number = Spark.instance.parallelism * 4

    if col_name is None:
        df = self.repartition(partitions_number)
    else:
        df = self.repartition(partitions_number, col_name)
    return df


@add_method(DataFrame)
def table_html(self, limit=100, columns=None):
    """
    Return a HTML table with the dataframe cols, data types and values
    :param self:
    :param columns: Columns to be printed
    :param limit: how many rows will be printed
    :return:
    """

    columns = parse_columns(self, columns)

    data = self.select(columns).limit(limit).to_json()

    # Load template
    path = os.path.dirname(os.path.abspath(__file__))
    template_loader = jinja2.FileSystemLoader(searchpath=path + "//../templates")
    template_env = jinja2.Environment(loader=template_loader, autoescape=True)
    template = template_env.get_template("table.html")

    # Filter only the columns and data type info need it
    dtypes = [(i[0], i[1], j.nullable,) for i, j in zip(self.dtypes, self.schema)]

    total_rows = self.count()
    if total_rows < limit:
        limit = total_rows

    total_rows = humanize.intword(total_rows)
    total_cols = self.cols.count()
    total_partitions = self.partitions()

    # Print table
    output = template.render(cols=dtypes, data=data, limit=limit, total_rows=total_rows, total_cols=total_cols,
                             partitions=total_partitions)
    return output


@add_method(DataFrame)
def table(self, limit=100, columns=None):
    try:
        __IPYTHON__
        result = self.table_html(limit=limit, columns=columns)
        return display(HTML(result))
    except NameError:
        self.show()


@add_method(DataFrame)
def correlation(self, columns, method="pearson", strategy="mean", output="json"):
    """
    Calculate the correlation between columns. It will try to cast a column to float where necessary and impute
    missing values
    :param self:
    :param columns: Columns to be processed
    :param method: Method used to calculate the correlation
    :param strategy: Imputing strategy
    :param output: array or json
    :return:
    """
    columns = parse_columns(self, columns)
    # try to parse the select column to float and create a vector

    df = self
    for col_name in columns:
        df = df.cols.cast(col_name, "float")
        logging.info("Casting {col_name} to float...".format(col_name=col_name))

    # Impute missing values
    imputed_cols = [c + "_imputed" for c in columns]
    df = df.cols.impute(columns, imputed_cols, strategy)
    logging.info("Imputing {columns}, Using '{strategy}'...".format(columns=columns, strategy=strategy))

    # Create Vector necessary to calculate the correlation
    df = df.cols.nest(imputed_cols, "features", "vector")

    corr = Correlation.corr(df, "features", method).head()[0].toArray()

    if output is "array":
        result = corr

    elif output is "json":

        # Parse result to json
        col_pair = []
        for col_name in columns:
            for col_name_2 in columns:
                col_pair.append({"between": col_name, "an": col_name_2})

        # flat array
        values = corr.flatten('F').tolist()

        result = []
        for n, v in zip(col_pair, values):
            # Remove correlation between the same column
            if n["between"] is not n["an"]:
                n["value"] = v
                result.append(n)

        result = sorted(result, key=lambda k: k['value'], reverse=True)

    return result


@add_method(DataFrame)
def create_id(self, column="id"):
    """
    Create a unique id for every row.
    :param self:
    :param column:
    :return:
    """

    return self.withColumn(column, F.monotonically_increasing_id())
