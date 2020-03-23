import ujson as json
import math

import humanize
import imgkit
import jinja2
import numpy as np
from dask_cudf.core import DataFrame

from optimus.engines.base.extension import BaseExt
from optimus.helpers.functions import random_int, traverse, absolute_path
from optimus.helpers.output import print_html
from optimus.helpers.raiseit import RaiseIt


def ext(self):
    class Ext(BaseExt):

        _name = None

        @staticmethod
        def cache():
            return self  # Dask.instance.persist(self)

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
                if isinstance(data_type, np.array):
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

            rows_count = self.rows.count()
            if n < rows_count:
                # n/rows_count can return a number that represent less the total number we expect. multiply by 1.1
                fraction = (n / rows_count) * 1.1
            else:
                fraction = 1.0
            return self.sample(frac=fraction, random_state=seed)

        @staticmethod
        def stratified_sample(col_name, seed: int = 1) -> DataFrame:
            """
            Stratified Sampling
            :param col_name:
            :param seed:
            :return:
            """
            df = self
            n = min(5, df[col_name].value_counts().min())
            df = df.groupby(col_name).apply(lambda x: x.sample(2))
            # df_.index = df_.index.droplevel(0)
            return df

        @staticmethod
        def pivot(index, column, values):
            """
            Return reshaped DataFrame organized by given index / column values.
            :param index: Column to use to make new frame's index.
            :param column: Column to use to make new frame's columns.
            :param values: Column(s) to use for populating new frame's values.
            :return:
            """
            raise NotImplementedError

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

            raise NotImplementedError

        @staticmethod
        def size():
            """
            Get the size of a spark in bytes
            :return:
            """

            return 0

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
            raise NotImplementedError

        @staticmethod
        def set_name(value=None):
            """
            Create a temp view for a data frame also used in the json output profiling
            :param value:
            :return:
            """
            self.ext._name = value
            # if not is_str(value):
            #     RaiseIt.type_error(value, ["string"])

            # if len(value) == 0:
            #     RaiseIt.value_error(value, ["> 0"])

            # self.createOrReplaceTempView(value)

        @staticmethod
        def get_name():
            """
            Get dataframe name
            :return:
            """
            return self.ext._name

        @staticmethod
        def partitions():
            return self.npartitions

        @staticmethod
        def partitioner():
            raise NotImplementedError

        @staticmethod
        def repartition(partitions_number=None, col_name=None):
            raise NotImplementedError

        @staticmethod
        def table_image(path, limit=10):
            """
            Output table as image
            :param limit:
            :param path:
            :return:
            """

            css = absolute_path("/css/styles.css")

            imgkit.from_string(Ext.table_html(limit=limit, full=True), path, css=css)
            print_html("<img src='" + path + "'>")

        @staticmethod
        def show():
            """
            Print df lineage
            :return:
            """
            return self.compute()
            # self.head()

        @staticmethod
        def debug():
            """
            Print df lineage
            :return:
            """
            raise NotImplementedError

        @staticmethod
        def create_id(column="id"):
            """
            Create a unique id for every row.
            :param column: Columns to be processed
            :return:
            """

            raise NotImplementedError

    return Ext(self)


DataFrame.ext = property(ext)
