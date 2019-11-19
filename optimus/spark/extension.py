import collections
import json

import humanize
import imgkit
import jinja2
import math
from packaging import version
from pyspark.ml.feature import SQLTransformer
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from optimus.bumblebee import Comm
from optimus.helpers.check import is_str
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import RELATIVE_ERROR
from optimus.helpers.converter import val_to_list
from optimus.helpers.functions import random_int, traverse, absolute_path
from optimus.helpers.json import json_converter
from optimus.helpers.output import print_html
from optimus.helpers.raiseit import RaiseIt
from optimus.profiler.profiler import Profiler
from optimus.profiler.templates.html import HEADER, FOOTER
from optimus.spark.spark import Spark

DataFrame.output = "html"


def ext(self):
    class Ext:

        @staticmethod
        def roll_out():
            """
            Just a function to check if the Spark spark has been Monkey Patched
            :return:
            """
            print(self)
            print("Yes!")

        @staticmethod
        def to_json():
            """
            Return a json from a Spark Dataframe
            :return:
            """
            return json.dumps(Ext.to_dict(), ensure_ascii=False, default=json_converter)

        @staticmethod
        def to_dict():
            """
            Return a dict from a Collect result
            [(col_name, row_value),(col_name_1, row_value_2),(col_name_3, row_value_3),(col_name_4, row_value_4)]
            :return:
            """
            # # Explore this approach seems faster
            # use_unicode = True
            # from pyspark.serializers import UTF8Deserializer
            # from pyspark.rdd import RDD
            # rdd = df._jdf.toJSON()
            # r = RDD(rdd.toJavaRDD(), df._sc, UTF8Deserializer(use_unicode))
            # if limit is None:
            #     r.collect()
            # else:
            #     r.take(limit)
            # return r
            #
            df = self
            dict_result = []

            # if there is only an element in the dict just return the value
            if len(dict_result) == 1:
                dict_result = next(iter(dict_result.values()))
            else:
                col_names = parse_columns(df, "*")

                # Because asDict can return messed columns names we order
                for row in df.collect():
                    _row = row.asDict()
                    r = collections.OrderedDict()
                    for col in col_names:
                        r[col] = _row[col]
                    dict_result.append(r)
            return dict_result

        @staticmethod
        def export():
            """
            Helper function to export all the spark in text format. Aimed to be used in test functions
            :return:
            """
            df = self
            dict_result = {}

            value = df.collect()
            schema = []
            for col_name in df.cols.names():

                data_type = df.cols.schema_dtype(col_name)
                if isinstance(data_type, ArrayType):
                    data_type = "ArrayType(" + str(data_type.elementType) + "()," + str(data_type.containsNull) + ")"
                else:
                    data_type = str(data_type) + "()"

                nullable = df.schema[col_name].nullable

                schema.append(
                    "('{name}', {dataType}, {nullable})".format(name=col_name, dataType=data_type, nullable=nullable))
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
        def stratified_sample(col_name, seed: int = 1) -> DataFrame:
            """
            Stratified Sampling
            :param col_name:
            :param seed:
            :return:
            """
            df = self
            fractions = df.select(col_name).distinct().withColumn("fraction", F.lit(0.8)).rdd.collectAsMap()
            # print(col_name, fractions, seed)
            df = df.stat.sampleBy(col_name, fractions, seed)
            return df

        @staticmethod
        def sample(n: int = 10, random: bool = False) -> DataFrame:
            """
            Return n number of samples from a dataFrame
            :param n: Number of samples
            :param random: if true get a semi random sample
            :return:
            """
            df = self
            if random is True:
                seed = random_int()
            elif random is False:
                seed = 0
            else:
                RaiseIt.value_error(random, ["True", "False"])

            rows_count = df.rows.count()
            if n < rows_count:
                # n/rows_count can return a number that represent less the total number we expect. multiply by 1.1 bo
                fraction = (n / rows_count) * 1.1
            else:
                fraction = 1.0
            # print(seed, n)
            return df.sample(False, fraction, seed=seed).limit(n)

        @staticmethod
        def pivot(index, column, values):
            """
            Return reshaped DataFrame organized by given index / column values.
            :param index: Column to use to make new frame's index.
            :param column: Column to use to make new frame's columns.
            :param values: Column(s) to use for populating new frame's values.
            :return:
            """
            return self.groupby(index).pivot(column).agg(F.first(values))

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

            df = self
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
            Get the size of a spark in bytes
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
                # TODO: Find a way to calculate the spark size in spark 2.4
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

            self.cache().count()
            return self

        @staticmethod
        def query(sql_expression):
            """
            Implements the transformations which are defined by SQL statement. Currently we only support
            SQL syntax like "SELECT ... FROM __THIS__ ..." where "__THIS__" represents the
            underlying table of the input spark.
            :param sql_expression: SQL expression.
            :return: Dataframe with columns changed by SQL statement.
            """
            sql_transformer = SQLTransformer(statement=sql_expression)
            return sql_transformer.transform(self)

        @staticmethod
        def set_name(value=None):
            """
            Create a temp view for a data frame also used in the json output profiling
            :param value:
            :return:
            """
            self.meta.set("name", value)
            if not is_str(value):
                RaiseIt.type_error(value, ["string"])

            if len(value) == 0:
                RaiseIt.value_error(value, ["> 0"])

            self.createOrReplaceTempView(value)

        @staticmethod
        def get_name():
            """
            Get spark name
            :return:
            """
            return Ext.get_meta("name")

        @staticmethod
        def partitions():
            """
            Return the spark partitions number
            :return: Number of partitions
            """
            return self.rdd.getNumPartitions()

        @staticmethod
        def partitioner():
            """
            Return the algorithm used to partition the spark

            :return:
            """
            return self.rdd.partitioner

        @staticmethod
        def repartition(partitions_number=None, col_name=None):
            """
            Apply a repartition to a datataframe based in some heuristics. Also you can pass the number of partitions and
            a column.
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
                df = self.repartition(partitions_number)
            else:
                df = self.repartition(partitions_number, col_name)
            return df

        @staticmethod
        def table_image(path, limit=10):
            """
            Output table as image
            :param limit:
            :param path:
            :return:
            """

            css = absolute_path("/css/styles.css")

            imgkit.from_string(self.table_html(limit=limit, full=True), path, css=css)
            print_html("<img src='" + path + "'>")

        @staticmethod
        def table_html(limit=10, columns=None, title=None, full=False, truncate=True, count=True):
            """
            Return a HTML table with the spark cols, data types and values
            :param columns: Columns to be printed
            :param limit: How many rows will be printed
            :param title: Table title
            :param full: Include html header and footer
            :param truncate: Truncate the row information
            :param count:

            :return:
            """

            df = self
            columns = parse_columns(df, columns)

            if limit is None:
                limit = 10

            if limit == "all":
                data = df.cols.select(columns).ext.to_dict()
            else:
                data = df.cols.select(columns).limit(limit).ext.to_dict()

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

            if count is True:
                total_rows = self.rows.approx_count()

                if limit == "all":
                    limit = total_rows
                elif total_rows < limit:
                    limit = total_rows

                total_rows = humanize.intword(total_rows)
            else:
                total_rows = None

            total_cols = self.cols.count()
            total_partitions = Ext.partitions()

            output = template.render(cols=final_columns, data=data, limit=limit, total_rows=total_rows,
                                     total_cols=total_cols,
                                     partitions=total_partitions, title=title, truncate=truncate)

            if full is True:
                output = HEADER + output + FOOTER
            return output

        @staticmethod
        def display(limit=None, columns=None, title=None, truncate=True):
            Ext.table(limit, columns, title, truncate)

        @staticmethod
        def table(limit=None, columns=None, title=None, truncate=True):
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

            try:
                if isnotebook() and DataFrame.output == "html":
                    result = Ext.table_html(title=title, limit=limit, columns=columns, truncate=truncate)
                    print_html(result)
                else:
                    self.show()
            except NameError:

                self.show()

        @staticmethod
        def show():
            """
            Print df lineage
            :return:
            """
            self.show()

        @staticmethod
        def debug():
            """
            Print df lineage
            :return:
            """
            print(self.rdd.toDebugString().decode("ascii"))

        @staticmethod
        def send(name: str = None, infer: bool = True, mismatch=None, stats: bool = True, advanced_stats: bool = True,
                 output: str = "http"):
            """
            Profile and send the data to the queue
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

        @staticmethod
        def reset():
            """
            Reset actions metadata and the profiler cache
            :return:
            """
            df = self.meta.set("transformations.actions", {})
            Profiler.instance.output_columns = {}
            return df

    return Ext()


DataFrame.ext = property(ext)
