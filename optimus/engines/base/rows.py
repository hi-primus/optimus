from abc import abstractmethod, ABC

from multipledispatch import dispatch

from optimus.engines.base.meta import Meta
# This implementation works for Spark, Dask, dask_cudf
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import one_list_to_val
from optimus.infer import is_str, is_list_of_str_or_int


class BaseRows(ABC):
    """Base class for all Rows implementations"""

    def __init__(self, root):
        self.root = root

    @staticmethod
    @abstractmethod
    def _sort(df, col_name, ascending):
        pass

    def _sort_multiple(self, dfd, meta, col_sort):
        """
        Sort rows taking into account multiple columns
        :param col_sort: column and sort type combination (col_name, "asc")
        :type col_sort: list of tuples
        """

        for cs in col_sort:
            col_name = one_list_to_val(cs[0])
            order = cs[1]

            if order != "asc" and order != "desc":
                RaiseIt.value_error(order, ["asc", "desc"])

            dfd = self._sort(dfd, col_name, True if order == "asc" else False)
            meta = Meta.action(meta, Actions.SORT_ROW.value, col_name)

        return dfd, meta

    def _reverse(self, dfd):
        return dfd[::-1]

    @staticmethod
    @abstractmethod
    def create_id(column="id"):
        pass

    @abstractmethod
    def append(self, dfs, cols_map):
        pass

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
        return self.root.new(dfd[self.root.mask.mismatch(input_col, dtype)])

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
        df = self.root
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
                where = eval(where)

        return df.cols.assign({output_col: where})

    def select(self, expr=None, contains=None, case=None, flags=0, na=False, regex=False):
        """
        :param expr: Expression used, For Ex: (df["A"] > 3) & (df["A"] <= 1000) or Column name "A"
        :param contains: List of string
        :param case:
        :param flags:
        :param na:
        :param regex:
        :return:
        """

        df = self.root
        dfd = df.data

        if is_str(expr):
            if expr in df.cols.names():
                if contains is not None:
                    expr = df.mask.contains(expr, value=contains, case=case, flags=flags, na=na, regex=regex)
                else:
                    expr = df[expr]
            else:
                expr = eval(expr)
        elif expr:
            expr = expr.get_series()
            dfd = dfd[expr]
        meta = Meta.action(df.meta, Actions.SELECT_ROW.value, df.cols.names())

        df = self.root.new(dfd, meta=meta)
        return df

    def _count(self, compute=True) -> int:
        return len(self.root.data.index)

    def count(self, compute=True) -> int:
        """
        Count dataframe rows
        """
        df = self.root
        dfd = df.data
        # TODO: Be sure that we need the compute param
        if compute is True:
            result = self._count(compute)
        else:
            result = df.functions.delayed(len)(dfd)
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

    @dispatch(str)
    def sort(self, input_col):
        df = self.root
        input_col = parse_columns(df, input_col)
        return df.rows.sort([(input_col, "desc",)])

    @dispatch(str, bool)
    def sort(self, input_col, asc=False):
        df = self.root
        input_col = parse_columns(df, input_col)
        return df.rows.sort([(input_col, "asc" if asc else "desc",)])

    @dispatch(str, str)
    def sort(self, input_col, order="desc"):
        df = self.root
        input_col = parse_columns(df, input_col)
        return df.rows.sort([(input_col, order,)])

    @dispatch(dict)
    def sort(self, col_sort):
        df = self.root
        return df.rows.sort([(k, v) for k, v in col_sort.items()])

    @dispatch(list, bool)
    def sort(self, input_col, asc=False):
        df = self.root
        return df.rows.sort(input_col, "asc" if asc else "desc")

    @dispatch(list)
    def sort(self, input_col):
        df = self.root
        return df.rows.sort(input_col)

    @dispatch(list, str)
    def sort(self, input_col, order="desc"):
        """
        Sort rows taking into account multiple columns
        :param input_col: column and sort type combination (col_name, "asc")
        :type input_col: list of tuples
        """
        # If a list of columns names are given order this by desc. If you need to specify the order of every
        # column use a list of tuples (col_name, "asc")
        df = self.root
        dfd = df.data
        meta = df.meta

        if is_list_of_str_or_int(input_col):
            t = []
            for col_name in input_col:
                t.append(tuple([col_name, order]))
            input_col = t

        dfd, meta = self._sort_multiple(dfd, meta, input_col)

        return self.root.new(dfd, meta=meta)

    def reverse(self):
        """

        :return:
        """
        dfd = self._reverse(self.root.data)
        return self.root.new(dfd)

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
                where = eval(where)
        dfd = dfd[where.get_series()==0]
        meta = Meta.action(df.meta, Actions.DROP_ROW.value, df.cols.names())
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
        subset = parse_columns(df, subset)
        meta = Meta.action(df.meta, Actions.DROP_ROW.value, df.cols.names())
        return self.root.new(df.data.dropna(how=how, subset=subset), meta=meta)

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

    def limit(self, count=10):
        """
        Limit the number of rows
        :param count:
        :return:
        """
        return self.root.new(self.root.data[:count])

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
