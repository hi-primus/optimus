import dask
import humanize

from optimus.engines.base.odataframe import BaseDataFrame
from optimus.helpers.columns import parse_columns
from optimus.helpers.functions import random_int
from optimus.helpers.raiseit import RaiseIt


class Ext(BaseDataFrame):

    def __init__(self, root, data):
        super().__init__(root, data)

    def new(self, df, meta):
        pass

    @staticmethod
    def delayed(func):
        def wrapper(*args, **kwargs):
            return dask.delayed(func)(*args, **kwargs)

        return wrapper

    def cache(self):
        df = self.data
        return self.new(df.persist(), meta=self)

    def compute(self):
        df = self.data
        return df.compute()

    def export(self):
        """
        Helper function to export all the dataframe in text format. Aimed to be used in test functions
        :return:
        """
        df = self.data
        df_data = df.to_json()
        df_schema = df.cols.dtypes()

        return f"{df_schema}, {df_data}"

    def sample(self, n=10, random=False):
        """
        Return a n number of sample from a dataFrame
        :param n: Number of samples
        :param random: if true get a semi random sample
        :return:
        """
        odf = self.root
        if random is True:
            seed = random_int()
        elif random is False:
            seed = 0
        else:
            RaiseIt.value_error(random, ["True", "False"])

        rows_count = odf.rows.count()
        if n < rows_count:
            # n/rows_count can return a number that represent less the total number we expect. multiply by 1.1
            fraction = (n / rows_count) * 1.1
        else:
            fraction = 1.0
        return self.root.new(odf.data.sample(frac=fraction, random_state=seed))

    def stratified_sample(self, col_name, seed: int = 1):
        """
        Stratified Sampling
        :param col_name:
        :param seed:
        :return:
        """
        df = self.data
        n = min(5, df[col_name].value_counts().min())
        df = df.groupby(col_name).apply(lambda x: x.sample(2))
        # df_.index = df_.index.droplevel(0)
        return self.root.new(df)

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
        df = self.data
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
        df = self.data
        df.cache().count()
        return df

    @staticmethod
    def query(sql_expression):
        raise NotImplementedError

    def partitions(self):
        df = self.data
        return df.npartitions

    @staticmethod
    def partitioner():
        print("Dask not support custom partitioner")
        raise NotImplementedError

    def show(self):
        """
        :return:
        """
        df = self.data
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
        odf = self.root
        columns = parse_columns(odf, columns)
        return odf.data[columns].head(n, npartitions=-1)

    @staticmethod
    def create_id(column="id"):
        """
        Create a unique id for every row.
        :param column: Columns to be processed
        :return:
        """

        raise NotImplementedError

    def to_dict(self, orient="records", index=True):
        """
        Create a dict
        :param orient:
        :param index: Return the series index
        :return:
        """

        series = self.data
        if index is True:
            return series.compute().to_dict(orient)
        else:
            return series.compute().to_list()

    def to_pandas(self):
        return self.data.compute()
