import json

import humanize
import imgkit
import jinja2
import math
from glom import glom, assign
from packaging import version
from pyspark.ml.feature import SQLTransformer
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from optimus import Comm
from optimus import val_to_list
from optimus.helpers.check import is_str
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import RELATIVE_ERROR
from optimus.helpers.decorators import *
from optimus.helpers.functions import collect_as_dict, random_int, traverse, absolute_path
from optimus.helpers.json import json_converter
from optimus.helpers.output import print_html
from optimus.helpers.raiseit import RaiseIt
from optimus.profiler.profiler import Profiler
from optimus.profiler.templates.html import HEADER, FOOTER
from optimus.spark import Spark

DataFrame._name = None
DataFrame.output = "html"


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
    return json.dumps(collect_as_dict(self), ensure_ascii=False, default=json_converter)


@add_method(DataFrame)
def to_dict(self):
    """
    Return a Python object from a Spark Dataframe
    :param self:
    :return:
    """
    return collect_as_dict(self)


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
    else:
        RaiseIt.value_error(random, ["True", "False"])

    rows_count = self.count()
    if n < rows_count:
        # n/rows_count can return a number that represent less the total number we expect. multiply by 1.1 bo
        fraction = (n / rows_count) * 1.1
    else:
        fraction = 1.0

    return self.sample(False, fraction, seed=seed).limit(n)


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
    :param data_type: All columns must have the same type. It will transform all columns to this data type.
    :return:
    """

    df = self
    id_vars = val_to_list(id_vars)
    # Cast all columns to the same type
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

    if version.parse(Spark.instance.spark.version) < version.parse("2.4.0"):
        java_obj = _to_java_object_rdd(self.rdd)
        n_bytes = Spark.instance.sc._jvm.org.apache.spark.util.SizeEstimator.estimate(java_obj)
    else:
        # TODO: Find a way to calculate the dataframe size in spark 2.4
        n_bytes = -1

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
def query(self, sql_expression):
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


@add_method(DataFrame)
def set_name(self, value=None):
    """
    Create a temp view for a data frame also used in the json output profiling
    :param self:
    :param value:
    :return:
    """
    self._name = value
    if not is_str(value):
        RaiseIt.type_error(value, ["string"])

    if len(value) == 0:
        RaiseIt.value_error(value, ["> 0"])

    self.createOrReplaceTempView(value)


@add_method(DataFrame)
def get_name(self):
    """
    Get dataframe name
    :param self:
    :return:
    """
    return self._name


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


# @add_attr(DataFrame)
# def glom(self):
#     """
#
#     :param self: Spark Dataframe
#     :return:
#     """
#     return collect_as_dict(self.rdd.glom().collect()[0])


@add_method(DataFrame)
def h_repartition(self, partitions_number=None, col_name=None):
    """
    Apply a repartition to a datataframe based in some heuristics. Also you can pass the number of partitions and
    a column if you need more control.
    See
    https://stackoverflow.com/questions/35800795/number-of-partitions-in-rdd-and-performance-in-spark/35804407#35804407
    https://medium.com/@adrianchang/apache-spark-partitioning-e9faab369d14
    https://umbertogriffo.gitbooks.io/apache-spark-best-practices-and-tuning/content/sparksqlshufflepartitions_draft.html
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
def table_image(self, path, limit=10):
    """

    :param self:
    :param limit:
    :param path:
    :return:
    """

    css = absolute_path("/css/styles.css")

    imgkit.from_string(self.table_html(limit=limit, full=True), path, css=css)
    print_html("<img src='" + path + "'>")


@add_method(DataFrame)
def table_html(self, limit=10, columns=None, title=None, full=False, truncate=True):
    """
    Return a HTML table with the dataframe cols, data types and values
    :param self:
    :param columns: Columns to be printed
    :param limit: How many rows will be printed
    :param title: Table title
    :param full: Include html header and footer
    :param truncate: Truncate the row information

    :return:
    """

    columns = parse_columns(self, columns)

    if limit is None:
        limit = 10

    if limit == "all":
        data = collect_as_dict(self.cols.select(columns))
    else:
        data = collect_as_dict(self.cols.select(columns).limit(limit))

    # Load the Jinja template
    template_loader = jinja2.FileSystemLoader(searchpath=absolute_path("/templates/out"))
    template_env = jinja2.Environment(loader=template_loader, autoescape=True)
    template = template_env.get_template("table.html")

    # Filter only the columns and data type info need it
    dtypes = []
    for i, j in zip(self.dtypes, self.schema):
        if i[1].startswith("array<struct"):
            dtype = "array<struct>"
        elif i[1].startswith("struct"):
            dtype = "struct"
        else:
            dtype = i[1]

        dtypes.append((i[0], dtype, j.nullable))

    # Remove not selected columns
    final_columns = []
    for i in dtypes:
        for j in columns:
            if i[0] == j:
                final_columns.append(i)

    total_rows = self.count()

    if limit == "all":
        limit = total_rows
    elif total_rows < limit:
        limit = total_rows

    total_rows = humanize.intcomma(total_rows)

    total_cols = self.cols.count()
    total_partitions = self.partitions()

    output = template.render(cols=final_columns, data=data, limit=limit, total_rows=total_rows, total_cols=total_cols,
                             partitions=total_partitions, title=title, truncate=truncate)

    if full is True:
        output = HEADER + output + FOOTER
    return output


def isnotebook():
    """
    Detect you are in a notebook or in a terminal
    :return:
    """
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True  # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


@add_method(DataFrame)
def table(self, limit=None, columns=None, title=None, truncate=True):
    try:
        if isnotebook() and DataFrame.output == "html":
            result = self.table_html(title=title, limit=limit, columns=columns, truncate=truncate)
            print_html(result)
        else:
            self.show()
    except NameError:

        self.show()


@add_method(DataFrame)
def random_split(self, weights: list = None, seed: int = 1):
    """
    Create 2 random splited DataFrames
    :param self:
    :param weights:
    :param seed:
    :return:
    """
    if weights is None:
        weights = [0.8, 0.2]

    return self.randomSplit(weights, seed)


@add_method(DataFrame)
def debug(self):
    """

    :param self:
    :return:
    """
    print(self.rdd.toDebugString().decode("ascii"))


@add_method(DataFrame)
def reset(self):
    df = self.set_meta("transformations.actions", {})
    Profiler.instance.output_columns = {}
    return df


@add_method(DataFrame)
def send(self, name=None, infer=True, mismatch=None, stats=True, advanced_stats=True, output="http"):
    """
    Profile and send the data to the queue
    :param self:
    :param infer: infer datatypes
    :param mismatch: a dict with the column name or regular expression to identify correct values.
    :param name: Specified a name for the view/dataframe
    :param stats: calculate stats or only vales
    :param output:
    :return:
    """
    df = self
    if name is not None:
        df.set_name(name)

    message = Profiler.instance.dataset(df, columns="*", buckets=35, infer=infer, relative_error=RELATIVE_ERROR,
                                        approx_count=True,
                                        sample=10000,
                                        stats=stats,
                                        format="json",
                                        mismatch=mismatch,
                                        advanced_stats=advanced_stats
                                        )

    if Comm.instance:
        return Comm.instance.send(message, output=output)
    else:
        raise Exception("Comm is not initialized. Please use comm=True param like Optimus(comm=True)")


@add_method(DataFrame)
def copy_meta(self, old_new_columns):
    """
    Shortcut to add transformations to a dataframe
    :param self:
    :param old_new_columns:
    :return:
    """

    key = "transformations.actions.copy"

    df = self

    copy_cols = df.get_meta(key)
    if copy_cols is None:
        copy_cols = {}
    copy_cols.update(old_new_columns)

    df = df.set_meta(key, copy_cols)

    return df


@add_method(DataFrame)
def rename_meta(self, old_new_columns):
    """
    Shortcut to add transformations to a dataframe
    :param self:
    :param old_new_columns:
    :return:
    """

    key = "transformations.actions.rename"

    df = self
    renamed_cols = df.get_meta(key)

    old, new = old_new_columns
    if renamed_cols is None or old not in list(renamed_cols.values()):
        df = df.update_meta(key, {old: new}, dict)
    else:
        # This update a key
        for k, v in renamed_cols.items():
            # print(old_new_columns)
            n, m = old_new_columns
            if v == n:
                renamed_cols[k] = m

        df = df.set_meta(key, renamed_cols)
    return df


@add_method(DataFrame)
def columns_meta(self, value):
    """
    Shortcut to add transformations to a dataframe
    :param self:
    :param value:
    :return:
    """
    df = self
    value = val_to_list(value)
    for v in value:
        df = df.update_meta("transformations.columns", v, list)
    return df


@add_method(DataFrame)
def action_meta(self, key, value):
    """
    Shortcut to add transformations to a dataframe
    :param self:
    :param key:
    :param value:
    :return:
    """
    df = self
    value = val_to_list(value)
    for v in value:
        df = df.update_meta("transformations.actions." + key, v, list)
    return df


@add_method(DataFrame)
def preserve_meta(self, old_df, key=None, value=None):
    """
    In some cases we need to preserve metadata actions before a destructive dataframe transformation.
    :param self: The target dataframe to get the metadata
    :param old_df: The Spark dataframe you want to coyp the metadata
    :param key:
    :param value:
    :return:
    """
    old_meta = old_df.get_meta()
    new_meta = self.get_meta()

    new_meta.update(old_meta)
    if key is None or value is None:
        return self.set_meta(value=new_meta)
    else:

        return self.set_meta(value=new_meta).action_meta(key, value)


@add_method(DataFrame)
def update_meta(df, path, value, default=list):
    """
    Append meta data to a key
    :param df:
    :param path:
    :param value:
    :param default:
    :return:
    """

    df = df

    new_meta = df.get_meta()
    if new_meta is None:
        new_meta = {}

    elements = path.split(".")
    result = new_meta
    for i, ele in enumerate(elements):
        if ele not in result and not len(elements) - 1 == i:
            result[ele] = {}

        if len(elements) - 1 == i:
            if default is list:
                result.setdefault(ele, []).append(value)
            elif default is dict:
                result.setdefault(ele, {}).update(value)
        else:
            result = result[ele]

    df = df.set_meta(value=new_meta)
    return df


@add_method(DataFrame)
def set_meta(self, spec=None, value=None, missing=dict):
    """
    Set metadata in a dataframe columns
    :param self:
    :param spec: path to the key to be modified
    :param value: dict value
    :param missing:
    :return:
    """
    if spec is not None:
        target = self.get_meta()
        data = assign(target, spec, value, missing=missing)
    else:
        data = value

    df = self
    df.schema[-1].metadata = data
    return df


@add_method(DataFrame)
def get_meta(self, spec=None):
    """
    Get metadata from a dataframe column
    :param self:
    :param spec: path to the key to be modified
    :return:
    """
    data = self.schema[-1].metadata
    if spec is not None:
        data = glom(data, spec, skip_exc=KeyError)
    return data
