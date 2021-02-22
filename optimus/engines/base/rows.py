from abc import abstractmethod, ABC

import pandas as pd

from optimus.engines.base.meta import Meta
# This implementation works for Spark, Dask, dask_cudf
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.infer import is_str


class BaseRows(ABC):
    """Base class for all Rows implementations"""

    def __init__(self, root):
        self.root = root

    @staticmethod
    @abstractmethod
    def create_id(column="id"):
        pass

    @abstractmethod
    def append(self, dfs, cols_map):
        pass

    #
    def greater_than(self, input_col, value):

        dfd = self.root.data
        return self.root.new(dfd[self.root.greather_than(input_col, value)])

    def greater_than_equal(self, input_col, value):

        dfd = self.root.data
        return self.root.new(dfd[self.root.greater_than_equal(input_col, value)])

    def less_than(self, input_col, value):

        dfd = self.root.data
        return self.root.new(dfd[self.root.less_than(input_col, value)])

    def less_than_equal(self, input_col, value):

        dfd = self.root.data
        return self.root.new(dfd[self.root.less_than_equal(input_col, value)])

    def equal(self, input_col, value):
        dfd = self.root.data
        return self.root.new(dfd[self.root.mask.is_equal(input_col, value)])

    def not_equal(self, input_col, value):

        dfd = self.root.data
        return self.root.new(dfd[self.root.mask.not_equal(input_col, value)])

    def missing(self, input_col):
        """
        Return missing values
        :param input_col:
        :return:
        """
        dfd = self.root.data
        return self.root.new(dfd[self.root.mask.missing(input_col)])

    def mismatch(self, input_col, dtype):
        """
        Return mismatches values
        :param input_col:
        :param dtype:
        :return:
        """
        dfd = self.root.data
        return self.root.new(dfd[self.root.mask.miasmatch(input_col, dtype)])

    def match(self, col_name, dtype):
        """
        Return Match values
        :param col_name:
        :param dtype:
        :return:
        """
        dfd = self.root.data
        return self.root.new(dfd[self.root.mask.match(col_name, dtype)])

    def apply(self, func, args=None, output_cols=None):
        """
        This will aimed to handle vectorized and not vectorized operations
        :param output_cols:
        :param func:
        :return:
        """
        dfd = self.root.data
        kw_columns = {}

        for output_col in output_cols:
            result = func(dfd, *args)
            kw_columns = {output_col: result}

        return df.cols.assign(kw_columns)

    def find(self, where, output_col):
        """
        Find rows and appends resulting mask to the dataset
        :param where: Mask, expression or name of the column to be taken as mask
        :param output_col:
        :return: Optimus Dataframe
        """

        df = self.root
        dfd = self.root.data

        if is_str(where):
            if where in df.cols.names():
                where = df[where]
            else:
                where = pd.eval(where)

        return df.cols.assign({output_col: where})

    def select(self, where):
        """
        :param where: Mask, expression or name of the column to be taken as mask
        :param expr: Expression used, For Ex: (df["A"] > 3) & (df["A"] <= 1000)
        :return:
        """

        df = self.root
        dfd = df.data

        if is_str(where):
            if where in df.cols.names():
                where = df[where]
            else:
                where = pd.eval(where)
        # dfd = dfd[where]
        dfd = dfd[where.data[where.cols.names()[0]]]
        meta = Meta.action(df.meta, Actions.SORT_ROW.value, df.cols.names())
        return self.root.new(dfd, meta=meta)

    def count(self, compute=True) -> int:
        """
        Count dataframe rows
        """
        dfd = self.root.data
        # TODO: Be sure that we need the compute param
        if compute is True:
            result = len(dfd.index)
        else:
            result = len(dfd.index)
        return result

    def to_list(self, input_cols):
        """

        :param input_cols:
        :return:
        """
        df = self.root
        input_cols = parse_columns(df, input_cols)
        value = df.cols.select(input_cols).to_pandas().values.tolist()

        return value

    @staticmethod
    @abstractmethod
    def sort(input_cols):
        pass

    def drop(self, where):
        """
        Drop rows depending on a mask or an expression
        :param where: Mask, expression or name of the column to be taken as mask
        :return: Optimus Dataframe
        """
        df = self.root
        dfd = df.data

        if is_str(where):
            if where in df.cols.names():
                where = df[where]
            else:
                where = pd.eval(where)
        # dfd = dfd[where]
        dfd = dfd[~where.data[where.cols.names()[0]]]
        meta = Meta.action(df.meta, Actions.SORT_ROW.value, df.cols.names())
        return self.root.new(dfd, meta=meta)

    @staticmethod
    @abstractmethod
    def between(columns, lower_bound=None, upper_bound=None, invert=False, equal=False,
                bounds=None):
        pass

    @staticmethod
    @abstractmethod
    def drop_by_dtypes(input_cols, data_type=None):
        pass

    def drop_na(self, subset=None, how="any", *args, **kwargs):
        """
        Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
        'any' of the values is null.
        :param subset:
        :param how:
        :return:
        """
        df = self.root
        subset = parse_columns(df.data, subset)
        df.meta = Meta.preserve(df.meta, df, Actions.DROP_ROW.value, df.cols.names())
        return self.root.new(df.dropna(how=how, subset=subset))

    @staticmethod
    @abstractmethod
    def drop_duplicates(input_cols=None):
        """
        Drop duplicates values in a dataframe
        :param input_cols: List of columns to make the comparison, this only  will consider this subset of columns,
        :return: Return a new DataFrame with duplicate rows removed
        :param input_cols:
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def limit(count):
        """
        Limit the number of rows
        :param count:
        :return:
        """

        pass

    def is_in(self, input_cols, values, output_cols=None):

        def _is_in(value, *args):
            _values = args
            return value.isin(_values)

        df = self.root
        return df.cols.apply(input_cols, func=_is_in, args=(values,), output_cols=output_cols)

    @staticmethod
    @abstractmethod
    def unnest(input_cols):
        pass

    def approx_count(self):
        """
        Aprox count
        :return:
        """
        return self.root.rows.count()
