import dask
import humanize
from dask.dataframe.core import DataFrame
from dask.dataframe.core import Series

from optimus.engines.base.commons.functions import to_float, to_integer, to_datetime
from optimus.engines.base.dataframe.extension import DataFrameSeriesBaseExt
from optimus.engines.base.extension import BaseExt
from optimus.helpers.columns import parse_columns
from optimus.helpers.functions import random_int
from optimus.helpers.raiseit import RaiseIt
from optimus.profiler.constants import MAX_BUCKETS


def ext(self: DataFrame):
    class Ext(BaseExt):

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
        def cast_and_profile(columns, bins: int = MAX_BUCKETS, output: str = None, flush: bool = False, size=False):
            """
            Helper function to infer, cast and profile a dataframe.
            :param columns:
            :param bins:
            :param output:
            :param flush:
            :param size:
            :return:
            """
            df = self
            cols_and_inferred_dtype = df.cols.infer_profiler_dtypes(columns)
            df = df.cols.cast_to_profiler_dtypes(columns=cols_and_inferred_dtype).persist()
            result = df.ext.profile(columns=columns, bins=bins, output=output, flush=flush, size=size)
            return df, result

        def export(self):
            """
            Helper function to export all the dataframe in text format. Aimed to be used in test functions
            :return:
            """
            df = self.df
            df_data = df.ext.to_json()
            df_schema = df.cols.dtypes()

            return f"{df_schema}, {df_data}"

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

        def stratified_sample(self, col_name, seed: int = 1):
            """
            Stratified Sampling
            :param col_name:
            :param seed:
            :return:
            """
            df = self.df
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

        def size(self, deep=False, format=None):
            """
            Get the size of a dask in bytes
            :return:
            """
            df = self.df
            result = df.memory_usage(index=True, deep=deep).sum().compute()
            if format == "human":
                result = humanize.naturalsize(result)

            return result

        def run(self):
            """
            This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
            evaluation approach in processing data: transformation functions are not computed into an action is called.
            Sometimes when transformations are numerous, the computations are very extensive because the high number of
            operations that spark needs to run in order to get the results.

            Other important thing is that Apache Spark save task but not result of dataFrame, so tasks are
            accumulated and the same situation happens.

            :return:
            """
            df = self.df
            df.cache().count()
            return df

        @staticmethod
        def query(sql_expression):
            raise NotImplementedError

        @staticmethod
        def partitions():
            df = self
            return df.npartitions

        @staticmethod
        def partitioner():
            print("Dask not support custom partitioner")
            raise NotImplementedError

        @staticmethod
        def show():
            """
            :return:
            """
            df = self
            return df.compute()

        @staticmethod
        def debug():
            """

            :return:
            """
            raise NotImplementedError

        def head(self, columns="*", n=10):
            """

            :return:
            """
            df = self.df
            columns = parse_columns(df, columns)
            return df[columns].head(n, npartitions=-1)

        @staticmethod
        def create_id(column="id"):
            """
            Create a unique id for every row.
            :param column: Columns to be processed
            :return:
            """

            raise NotImplementedError

    return Ext(self)


def ext_series(self: Series):
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
                return series.compute().to_dict()
            else:
                return series.compute().to_list()

        def __init__(self, series):
            super(Ext, self).__init__(series)
            self.series = series

        def to_float(self):
            series = self.series
            return series.map(to_float)

        def to_integer(self):
            series = self.series
            return series.map(to_integer)

        def to_datetime(self, format):
            series = self.series
            return to_datetime(series, format)

    return Ext(self)


DataFrame.ext = property(ext)
Series.ext = property(ext_series)
