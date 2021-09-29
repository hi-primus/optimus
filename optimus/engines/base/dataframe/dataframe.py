from collections.abc import Iterable

import humanize

from optimus.engines.base.basedataframe import BaseDataFrame
from optimus.helpers.functions import random_int
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_int, is_list_value


class DataFrameBaseDataFrame(BaseDataFrame):

    def execute(self):
        df = self.data
        return self.new(df, meta=self.meta)

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

        if n > rows_count:
            n = rows_count
        dfd = df.data.sample(n=n, random_state=seed).reset_index(drop=True)

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

    def stack(self, index=None, col_name="variable", value_name="value"):
        """
        Return reshaped DataFrame organized by given index / column values.
        :param index: Column(s) to use as index.
        :param col_name: Column name for stacked columns
        :param value_name: Column name for values        
        :return:
        """
        dfd = self.root.data
        new_level = '0'

        if index:
            dfd = dfd.set_index(index)
            new_level = '1'
            if is_list_value(index):
                new_level = str(len(index))

        dfd = dfd.stack().reset_index()

        def _join(val):
            if isinstance(val, Iterable):
                return ''.join([str(s) if s else '' for s in val])
            else:
                return str(val)

        dfd.columns = dfd.columns.map(_join).str.strip('_')

        return self.root.new(dfd).cols.rename([
            ("level_" + new_level, col_name),
            ("0", value_name)
        ])

    def unstack(self, index=None, level=-1):
        """
        Return reshaped DataFrame organized by given index / column values.
        :param index: Column(s) to use as index.
        :param level: Column(s) to unstack.
        :return:
        """
        dfd = self.root.data

        dfd = dfd.set_index(index)
        dfd = dfd.unstack(level=level, fill_value=None).reset_index()

        def _join(val):
            if isinstance(val, Iterable):
                return list(filter(lambda l: l, val))[-1]
            else:
                return str(val)

        dfd.columns = dfd.columns.map(_join).str.strip('_')

        return self.root.new(dfd)

    def melt(self, id_cols=None, value_cols=None, var_name="variable", value_name="value", data_type=None):
        """
        Convert DataFrame from wide to long format.
        :param id_cols: Column(s) with unique values
        :param value_cols: Column names that are going to be converted to columns values
        :param var_name: Column name for vars
        :param value_name: Column name for values
        :param data_type: Transforms all columns to this data type.
        :return:
        """
        dfd = self.root.data
        dfd = dfd.melt(id_vars=id_cols, value_vars=value_cols, var_name=var_name, value_name=value_name)
        return self.root.new(dfd)

    def size(self, deep=False, format=None):
        """
        Get the size of a dask in bytes
        :return:
        """
        df = self.data
        result = df.memory_usage(index=True, deep=deep).sum()
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
        return 1

    @staticmethod
    def partitioner():
        print("Dask not support custom partitioner")
        raise NotImplementedError

    @staticmethod
    def debug():
        """

        :return:
        """
        raise NotImplementedError

    def to_pandas(self):
        return self.data

    def to_delayed(self):
        return self.functions.to_delayed(self.data)
