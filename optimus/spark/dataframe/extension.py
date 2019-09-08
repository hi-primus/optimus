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

from optimus import val_to_list
from optimus.bumblebee import Comm
from optimus.spark.helpers.check import is_str
from optimus.spark.helpers.columns import parse_columns
from optimus.spark.helpers.constants import RELATIVE_ERROR
from optimus.spark.helpers import collect_as_dict, random_int, traverse, absolute_path
from optimus.spark.helpers import json_converter
from optimus.spark.helpers.output import print_html
from optimus.spark.helpers.raiseit import RaiseIt
from optimus.profiler.profiler import Profiler
from optimus.profiler.templates.html import HEADER, FOOTER
from optimus.spark import Spark

DataFrame.output = "html"


def ext(self):
    source_df = self

    class Ext:
        def __init__(self):
            self._name = None

        @staticmethod
        def roll_out():
            """
            Just a function to check if the Spark dataframe has been Monkey Patched
            :return:
            """
            print("Yes!")

        @staticmethod
        def to_json():
            """
            Return a json from a Spark Dataframe
            :return:
            """
            return json.dumps(collect_as_dict(source_df), ensure_ascii=False, default=json_converter)

        @staticmethod
        def to_dict():
            """
            Return a Python object from a Spark Dataframe
            :return:
            """
            return collect_as_dict(source_df)

        @staticmethod
        def export():
            """
            Helper function to export all the dataframe in text format. Aimed to be used in test functions
            :return:
            """
            dict_result = {}
            value = source_df.collect()
            schema = []
            for col_names in source_df.cols.names():
                name = col_names

                data_type = source_df.cols.schema_dtype(col_names)
                if isinstance(data_type, ArrayType):
                    data_type = "ArrayType(" + str(data_type.elementType) + "()," + str(data_type.containsNull) + ")"
                else:
                    data_type = str(data_type) + "()"

                nullable = source_df.schema[col_names].nullable

                schema.append(
                    "('{name}', {dataType}, {nullable})".format(name=name, dataType=data_type, nullable=nullable))
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

        @staticmethod
        def sample(n=10, random=False):
            """
            Return a n number of sample from a dataFrame
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

            rows_count = source_df.count()
            if n < rows_count:
                # n/rows_count can return a number that represent less the total number we expect. multiply by 1.1 bo
                fraction = (n / rows_count) * 1.1
            else:
                fraction = 1.0

            return source_df.sample(False, fraction, seed=seed).limit(n)

        @staticmethod
        def pivot(index, column, values):
            """
            Return reshaped DataFrame organized by given index / column values.
            :param index: Column to use to make new frame's index.
            :param column: Column to use to make new frame's columns.
            :param values: Column(s) to use for populating new frame's values.
            :return:
            """
            return source_df.groupby(index).pivot(column).agg(F.first(values))

        @staticmethod
        def melt(id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
            """
            Convert DataFrame from wide to long format.
            :param id_vars: column with unique values
            :param value_vars: Column names that are going to be converted to columns values
            :param var_name: Column name for vars
            :param value_name: Column name for values
            :param data_type: All columns must have the same type. It will transform all columns to this data type.
            :return:
            """

            df = source_df
            id_vars = val_to_list(id_vars)
            # Cast all columns to the same type
            df = df.cols.cast(id_vars + value_vars, data_type)

            vars_and_vals = [F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name)) for c in value_vars]

            # Add to the DataFrame and explode
            df = df.withColumn("vars_and_vals", F.explode(F.array(*vars_and_vals)))

            cols = id_vars + [F.col("vars_and_vals")[x].alias(x) for x in [var_name, value_name]]

            return df.select(*cols)

        @staticmethod
        def size():
            """
            Get the size of a dataframe in bytes

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
                java_obj = _to_java_object_rdd(source_df.rdd)
                n_bytes = Spark.instance.sc._jvm.org.apache.spark.util.SizeEstimator.estimate(java_obj)
            else:
                # TODO: Find a way to calculate the dataframe size in spark 2.4
                n_bytes = -1

            return n_bytes

        @staticmethod
        def run():
            """
            This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
            evaluation approach in processing data: transformation functions are not computed into an action is called.
            Sometimes when transformations are numerous, the computations are very extensive because the high number of
            operations that spark needs to run in order to get the results.

            Other important thing is that Apache Spark save task but not result of dataFrame, so tasks are
            accumulated and the same situation happens.

            :return:
            """

            source_df.cache().count()
            return source_df

        @staticmethod
        def query(sql_expression):
            """
            Implements the transformations which are defined by SQL statement. Currently we only support
            SQL syntax like "SELECT ... FROM __THIS__ ..." where "__THIS__" represents the
            underlying table of the input dataframe.
            :param sql_expression: SQL expression.
            :return: Dataframe with columns changed by SQL statement.
            """
            sql_transformer = SQLTransformer(statement=sql_expression)
            return sql_transformer.transform(source_df)

        @property
        def name(self):
            """
            Get dataframe name
            :return:
            """
            return self._name

        @name.setter
        def name(self, value=None):
            """
            Create a temp view for a data frame also used in the json output profiling
            :param value:
            :return:
            """
            source_df._name = value
            if not is_str(value):
                RaiseIt.type_error(value, ["string"])

            if len(value) == 0:
                RaiseIt.value_error(value, ["> 0"])

            self._name = value
            source_df.createOrReplaceTempView(value)

        @staticmethod
        def partitions():
            """
            Return the dataframe partitions number
            :return: Number of partitions
            """
            return source_df.rdd.getNumPartitions()

        @staticmethod
        def partitioner():
            """
            Return the algorithm used to partition the dataframe
            :return:
            """
            return source_df.rdd.partitioner

        @staticmethod
        def repartition(partitions_number=None, col_name=None):
            """
            Apply a repartition to a datataframe based in some heuristics. Also you can pass the number of partitions and
            a column if you need more control.
            See
            https://stackoverflow.com/questions/35800795/number-of-partitions-in-rdd-and-performance-in-spark/35804407#35804407
            https://medium.com/@adrianchang/apache-spark-partitioning-e9faab369d14
            https://umbertogriffo.gitbooks.io/apache-spark-best-practices-and-tuning/content/sparksqlshufflepartitions_draft.html
            :param partitions_number: Number of partitions
            :param col_name: Column to be used to apply the repartition id necessary
            :return:
            """

            if partitions_number is None:
                partitions_number = Spark.instance.parallelism * 4

            if col_name is None:
                df = source_df.repartition(partitions_number)
            else:
                df = source_df.repartition(partitions_number, col_name)
            return df

        @staticmethod
        def table_image(path, limit=10):
            """

            :param limit:
            :param path:
            :return:
            """

            css = absolute_path("/css/styles.css")

            imgkit.from_string(source_df.table_html(limit=limit, full=True), path, css=css)
            print_html("<img src='" + path + "'>")

        @staticmethod
        def table_html(limit=10, columns=None, title=None, full=False, truncate=True):
            """
            Return a HTML table with the dataframe cols, data types and values
            :param columns: Columns to be printed
            :param limit: How many rows will be printed
            :param title: Table title
            :param full: Include html header and footer
            :param truncate: Truncate the row information

            :return:
            """

            columns = parse_columns(source_df, columns)

            if limit is None:
                limit = 10

            if limit == "all":
                data = collect_as_dict(source_df.cols.select(columns))
            else:
                data = collect_as_dict(source_df.cols.select(columns).limit(limit))

            # Load the Jinja template
            template_loader = jinja2.FileSystemLoader(searchpath=absolute_path("/templates/output"))
            template_env = jinja2.Environment(loader=template_loader, autoescape=True)
            template = template_env.get_template("table.html")

            # Filter only the columns and data type info need it
            dtypes = []
            for i, j in zip(source_df.dtypes, source_df.schema):
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

            total_rows = source_df.rows.approx_count()

            if limit == "all":
                limit = total_rows
            elif total_rows < limit:
                limit = total_rows

            total_rows = humanize.intword(total_rows)

            total_cols = source_df.cols.count()
            total_partitions = source_df.partitions()

            output = template.render(cols=final_columns, data=data, limit=limit, total_rows=total_rows,
                                     total_cols=total_cols,
                                     partitions=total_partitions, title=title, truncate=truncate)

            if full is True:
                output = HEADER + output + FOOTER
            return output

        @staticmethod
        def table(limit=None, columns=None, title=None, truncate=True):
            try:
                if __IPYTHON__ and DataFrame.output is "html":
                    result = source_df.table_html(title=title, limit=limit, columns=columns, truncate=truncate)
                    print_html(result)
                else:
                    source_df.show()
            except NameError:

                source_df.show()

        @staticmethod
        def debug():
            """
            Print rdd lineage
            :return:
            """
            print(source_df.rdd.toDebugString().decode("ascii"))

        @staticmethod
        def create_id(column="id"):
            """
            Create a unique id for every row.
            :param column: Columns to be processed
            :return:
            """

            return source_df.withColumn(column, F.monotonically_increasing_id())

        @staticmethod
        def send(name=None, stats=True):
            """
            Profile and send the data to the queue
            :param name: Specified a name for the view/dataframe
            :param stats:
            :return:
            """
            df = source_df
            if name is not None:
                df.set_name(name)

            result = Profiler.instance.dataset(df, columns="*", buckets=35, infer=False, relative_error=RELATIVE_ERROR,
                                               approx_count=True,
                                               sample=10000,
                                               stats=stats)

            Comm.instance.send(result)

        @staticmethod
        def append_meta(path, value):
            target = source_df.get_meta()
            data = glom(target, (path, T.append(value)))

            df = source_df
            df.schema[-1].metadata = data
            return df

        @property
        def meta(self, target=None):
            """
            Get metadata from a dataframe column
            :param target: Path to key to be get
            :return:
            """
            data = source_df.schema[-1].metadata
            if target is not None:
                data = glom(data, target, skip_exc=KeyError)
            return data

        @meta.setter
        def meta(self, target=None, value=None, missing=dict):
            """
            Set metadata in a dataframe columns
            :param target: Path to key to be set
            :param value: Value to be set
            :param missing:
            :return:
            """
            if target is not None:
                target = source_df.get_meta()
                data = assign(target, target, value, missing=missing)
            else:
                data = value

            df = source_df
            df.schema[-1].metadata = data
            # return df

    return Ext()
