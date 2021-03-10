from abc import abstractmethod

import humanize

from dask.distributed import Variable

from optimus.engines.base.basedataframe import BaseDataFrame
from optimus import engines as Engine
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.columns import parse_columns
from optimus.helpers.functions import random_int, parse_size
from optimus.helpers.output import output_image, output_base64
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_one_element
import dask


class DaskBaseDataFrame(BaseDataFrame):

    def __init__(self, root, data):
        super().__init__(root, data)

    def _assign(self, kw_columns):

        dfd = self.root.data

        if dfd.known_divisions:
            for key in kw_columns:
                kw_column = kw_columns[key]
                if not is_one_element(kw_column) and not callable(kw_column) and not kw_column.known_divisions:
                    kw_columns[key] = kw_column.reset_index().set_index('index')[key]
        return dfd.assign(**kw_columns)

    @staticmethod
    @abstractmethod
    def _pandas_to_dfd(pdf, n_partitions):
        pass

    def execute(self):
        self.data = self.data.persist()
        return self

    def compute(self):
        df = self.data
        return df.compute()

    def visualize(self):
        return display(self.data.visualize())

    def export(self):
        """
        Helper function to export all the dataframe in text format. Aimed to be used in test functions
        :return:
        """
        df = self.data
        df_data = df.to_json()
        df_schema = df.cols.dtypes()

        return f"{df_schema}, {df_data}"

    def _reset_buffer(self):
        if self.buffer:
            Variable(self.buffer).delete()
            self.buffer = None

    def _create_buffer_df(df, input_cols, n):
        import uuid
        unique_id = str(uuid.uuid4())
        Variable(unique_id).set(PandasDataFrame(df.cols.select(input_cols).head(n=n)))
        return unique_id

    def get_buffer(self):
        return Variable(self.buffer).get() if self.buffer else None

    def _buffer_window(df, input_cols, lower_bound, upper_bound):
        return PandasDataFrame(df.get_buffer().data[input_cols][lower_bound: upper_bound])

    def sample(self, n=10, random=False):
        """
        Return a n number of sample from a dataFrame
        :param n: Number of samples
        :param random: if true get a semi random sample
        :return:
        """
        df = self.root
        if random is True:
            seed = random_int()
        elif random is False:
            seed = 0
        else:
            RaiseIt.value_error(random, ["True", "False"])

        rows_count = df.rows.count()
        if n < rows_count:
            # n/rows_count can return a number that represent less the total number we expect. multiply by 1.1
            fraction = (n / rows_count) * 1.1
        else:
            fraction = 1.0
        return self.root.new(df.data.sample(frac=fraction, random_state=seed))

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
        return self.data.npartitions

    @staticmethod
    def partitioner():
        print("Dask not support custom partitioner")
        raise NotImplementedError

    def repartition(self, n=None, *args, **kwargs):
        dfd = self.data
        df = self
        if n == "auto":
            # Follow a heuristic for partitioning a mentioned
            # https://docs.dask.org/en/latest/best-practices.html#avoid-very-large-partitions
            client = dask.distributed.get_client()
            worker_memory = parse_size(client.cluster.worker_spec[0]["options"]["memory_limit"])
            nthreads = client.cluster.worker_spec[0]["options"]["nthreads"]

            part_recommended_size = worker_memory / nthreads / 10
            n = int(df.size() / part_recommended_size)
            # Partition can not be lower than 1
            n = n if n < 0 else 1

            dfd = dfd.repartition(npartitions=n, *args, **kwargs)
        else:
            dfd = dfd.repartition(npartitions=n, *args, **kwargs)

        return self.new(dfd, meta=self.meta)

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
        df = self.root
        columns = parse_columns(df, columns)
        return df.data[columns].head(n, npartitions=-1)

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

    @property
    def constants(self):
        from optimus.engines.base.dask.constants import constants
        return constants(self)
