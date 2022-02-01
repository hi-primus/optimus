from abc import abstractmethod

import dask
import dask.array as da
import dask.dataframe as dd
import humanize
import numpy as np
import pandas as pd
from distributed import Variable
from dask.utils import parse_bytes

from optimus.engines.base.basedataframe import BaseDataFrame
from optimus.engines.base.distributed.dataframe import DistributedBaseDataFrame
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import random_int
from optimus.helpers.raiseit import RaiseIt
from optimus.helpers.types import *
from optimus.infer import is_int


class DaskBaseDataFrame(DistributedBaseDataFrame):

    def _assign(self, kw_columns: dict):

        dfd = self.root.data

        for key in kw_columns:
            kw_column = kw_columns[key]

            if isinstance(kw_column, (list,)):
                kw_column = pd.Series(kw_column)

            if isinstance(kw_column, pd.Series):
                # TO-DO: A Pandas Series should be assignable to a Dask DataFrame
                kw_column = dd.from_pandas(kw_column, npartitions=dfd.npartitions)

            if isinstance(kw_column, (np.ndarray, da.Array)):
                kw_column = dd.from_array(kw_column)

            if isinstance(kw_column, (dd.Series, dd.DataFrame)):

                if isinstance(kw_column, dd.DataFrame):
                    if key in kw_column:
                        # the incoming series has the same column key
                        kw_column = kw_column[key]
                    else:
                        # the incoming series has no column key
                        kw_column = kw_column[list(kw_column.columns)[0]]
                else:
                    kw_column.name = key

            kw_columns[key] = kw_column

        kw_columns = {str(key): kw_column for key, kw_column in kw_columns.items()}
        return dfd.assign(**kw_columns)

    @staticmethod
    @abstractmethod
    def _base_to_dfd(df, n_partitions):
        """
        Convert a dataframe from the dataframe base (pandas, cudf) in a distributed engine dataframe (dask, dask_cudf)
        :param pdf:
        :param n_partitions:
        :return:
        """
        pass

    def execute(self):
        self.data = self.data.persist()
        return self

    def compute(self):
        df = self.data
        return df.compute()

    def visualize(self):
        return display(self.data.visualize())

    def _iloc(self, input_cols, lower_bound, upper_bound):
        def func(value):
            return value[lower_bound:upper_bound].reset_index(drop=True)

        return self.root.new(self.data[input_cols].partitions[0].map_partitions(func))

    def graph(self) -> dict:
        """
        Return a dict the Dask tasks graph
        :return:
        """
        return self.data.__dask_graph__().layers

    def sample(self, n=10, random=False):
        """
        Return a n number of sample from a dataFrame
        :param n: Number of samples
        :param random: if true get a semi random sample
        :return:
        """
        if not is_int(n):
            RaiseIt.type_error(n, ["int"])

        if random is True:
            seed = int(random_int())
        elif random is False:
            seed = 0
        elif is_int(random):
            seed = random
        else:
            RaiseIt.type_error(random, ["bool", "int"])

        df = self.root
        rows_count = df.rows.count()
        if n < rows_count:
            # n/rows_count can return a number that represent less the total number we expect. multiply by 1.001
            fraction = (n / rows_count) * 1.001
        else:
            fraction = 1.0

        dfd = df.data.sample(frac=fraction, random_state=seed).reset_index(drop=True)

        return self.root.new(dfd)

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

    def pivot(self, col, groupby, agg=None, values=None):
        """
        Return reshaped DataFrame organized by given index / column values.
        :param col: Column to use to make new frame's columns.
        :param groupby: Column to use to make new frame's index.
        :param agg: Aggregation to use for populating new frame's values.
        :param values: Column to use for populating new frame's values.
        :return:
        """

        df = self.root

        if values is not None:
            agg = ("first", values)

        groupby = val_to_list(groupby)
        by = groupby + [col]
        if agg is None:
            agg = ("count", col)
        return df.cols.groupby(by=by, agg=agg).unstack(by)

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

    def equals_dataframe(self, df2: 'DataFrameType') -> bool:
        if isinstance(df2, (BaseDataFrame,)):
            return df2.to_dict(n="all") == self.to_dict(n="all")
        else:
            return self.to_dict(n="all") == df2

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
            client = distributed.get_client()
            worker_memory = parse_bytes(client.cluster.worker_spec[0]["options"]["memory_limit"])
            nthreads = client.cluster.worker_spec[0]["options"]["nthreads"]

            part_recommended_size = worker_memory / nthreads / 10
            n = int(df.size() / part_recommended_size)

            # Partition can not be lower than 1
            n = n if n < 0 else 1
            # TODO .repartition(partition_size="100MB"). https://stackoverflow.com/questions/44657631/strategy-for-partitioning-dask-dataframes-efficiently

        if n > 0:
            dfd = dfd.repartition(npartitions=n, *args, **kwargs)

        return self.new(dfd, meta=self.meta)

    @staticmethod
    def debug():
        """

        :return:
        """
        raise NotImplementedError

    def to_dict(self, cols="*", n=10, orient="list"):
        """
        Create a dict
        :param n:
        :param orient:
        :return:
        """

        if n == "all":
            series = self.cols.select(cols).to_pandas()
        else:
            series = self.iloc(cols, 0, n).to_pandas()

        return series.to_dict(orient)

    def to_pandas(self):
        return self.data.compute()

    @property
    def constants(self):
        from optimus.engines.base.dask.constants import Constants
        return Constants()
