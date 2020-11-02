import dask
import imgkit
from dask_cudf import DataFrame as DaskCUDFDataFrame
from dask_cudf import Series as DaskCUDFSeries

from optimus.engines.base.commons.functions import to_datetime, to_float_cudf, to_integer_cudf
from optimus.engines.base.dataframe.extension import DataFrameSeriesBaseExt
from optimus.engines.base.extension import BaseExt
from optimus.helpers.functions import random_int, absolute_path
from optimus.helpers.output import print_html
from optimus.helpers.raiseit import RaiseIt


def ext(self):
    class Ext(BaseExt):

        # _name = None
        def __init__(self, df):
            super().__init__(df)
            self.df = df

        @staticmethod
        def delayed(func):
            def wrapper(*args, **kwargs):
                return dask.delayed(func)(*args, **kwargs)

            return wrapper

        def cache(self):
            df = self.df
            return df.persist()

        def compute(self):
            df = self.df
            return df.compute()

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
        def stratified_sample(col_name, seed: int = 1):
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

        #
        # @staticmethod
        # def repartition(n=0, col_name=None):
        #     return df.repartition(n)

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

        @staticmethod
        def is_cached(df):
            """

            :return:
            """
            return False if df.meta.get("profile") is None else True

    return Ext(self)


def ext_series(self: DaskCUDFSeries):
    class Ext(DataFrameSeriesBaseExt):
        @staticmethod
        def delayed(func):
            def wrapper(*args, **kwargs):
                return dask.delayed(func)(*args, **kwargs)

            return wrapper

        def to_dict(self, index=True):
            """
            Create a dict
            :param index: Return the series index
            :return:
            """
            series = self.series
            if index is True:
                return series.compute().to_pandas().to_dict()
            else:
                return series.compute().to_arrow().to_pylist()

        def __init__(self, series):
            super(Ext, self).__init__(series)
            self.series = series

        def to_float(self):
            series = self.series
            return series.map_partitions(to_float_cudf)

        def to_integer(self):
            series = self.series
            return series.map_partitions(to_integer_cudf)

        def to_datetime(self, format):
            series = self.series
            return to_datetime(series, format)

    return Ext(self)


DaskCUDFDataFrame.ext = property(ext)
DaskCUDFSeries.ext = property(ext_series)
