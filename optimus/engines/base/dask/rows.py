import functools
import operator

import dask.array as da
import dask.dataframe as  dd
# from dask_cudf.core import DataFrame
from multipledispatch import dispatch

from optimus.audf import filter_row_by_data_type as fbdt
from optimus.engines.base.rows import BaseRows
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str_or_int


# This implementation works for Spark, Dask, dask_cudf
class DaskBaseRows(BaseRows):
    """Base class for all Rows implementations"""

    def __init__(self, df):
        super(DaskBaseRows, self).__init__(df)

    def create_id(self, column="id"):
        # Reference https://github.com/dask/dask/issues/1426
        df = self.df
        # print(df)
        a = da.arange(df.divisions[-1] + 1, chunks=df.divisions[1:])
        df[column] = dd.from_dask_array(a)
        return df

    def append(self, rows):
        """

        :param rows:
        :return:
        """
        # df = self.df
        df = dd.concat([self, rows], axis=0)

        return df

    def limit(self, count):
        """
        Limit the number of rows
        :param count:
        :return:
        """

        # @dask.delayed
        # def _limit(df, count):
        #     return df.head(count)
        #
        # df = self.df
        # return _limit(df, count)
        df = self.df
        return df.head(count)

    def select(self, condition):
        """

        :param condition: a condition like (df.A > 0) & (df.B <= 10)
        :return:
        """
        df = self.df
        df = df[condition]
        df = df.meta.preserve(df, Actions.SORT_ROW.value, df.cols.names())

        return df

    def select_by_dtypes(self, input_cols, data_type=None):

        input_cols = parse_columns(self, input_cols)
        return self.select(fbdt(input_cols, data_type))

    def count(self, compute=True) -> int:
        """
        Count dataframe rows
        """
        df = self.df
        if compute is True:
            result = len(df.compute())
        else:
            result = len(df)
        return result

    def to_list(self, input_cols):
        """

        :param input_cols:
        :return:
        """
        df = self.df
        input_cols = parse_columns(df, input_cols)
        df = df[input_cols].compute().values.tolist()

        return df

    @dispatch(str, str)
    def sort(self, input_cols):
        df = self.df
        input_cols = parse_columns(df, input_cols)
        return df.rows.sort([(input_cols, "desc",)])

    @dispatch(str, str)
    def sort(self, columns, order="desc"):
        """
        Sort column by row
        """
        df = self.df
        columns = parse_columns(df, columns)
        return df.rows.sort([(columns, order,)])

    @dispatch(list)
    def sort(self, col_sort):
        """
        Sort rows taking into account multiple columns
        :param col_sort: column and sort type combination (col_name, "asc")
        :type col_sort: list of tuples
        """
        # If a list of columns names are given order this by desc. If you need to specify the order of every
        # column use a list of tuples (col_name, "asc")
        df = self.df

        t = []
        if is_list_of_str_or_int(col_sort):
            for col_name in col_sort:
                t.append(tuple([col_name, "desc"]))
            col_sort = t

        for cs in col_sort:
            col_name = one_list_to_val(cs[0])
            order = cs[1]

            if order != "asc" and order != "asc":
                RaiseIt.value_error(order, ["asc", "desc"])

            df = df.meta.preserve(df, Actions.SORT_ROW.value, col_name)

            c = df.cols.names()
            # It seems that is on possible to order rows in Dask using set_index. It only return data in asc way.
            # We should fins a way to make it work desc and form multiple columns
            df = df.set_index(col_name).reset_index()[c]

        return df

    def drop(self, where=None):
        """
        Drop a row depending on a dataframe expression
        :param where: Expression used to drop the row, For Ex: (df.A > 3) & (df.A <= 1000)
        :return: Spark DataFrame
        :return:
        """
        df = self.df
        df = df[where]
        df = df.meta.preserve(df, Actions.DROP_ROW.value, df.cols.names())
        return df

    def between_index(self, columns, lower_bound=None, upper_bound=None):
        """

        :param columns:
        :param lower_bound:
        :param upper_bound:
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        return df[lower_bound: upper_bound][columns]

    def between(self, columns, lower_bound=None, upper_bound=None, invert=False, equal=False,
                bounds=None):
        """
        Trim values at input thresholds
        :param upper_bound:
        :param lower_bound:
        :param columns: Columns to be trimmed
        :param invert:
        :param equal:
        :param bounds:
        :return:
        """
        df = self.df
        # TODO: should process string or dates
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        if bounds is None:
            bounds = [(lower_bound, upper_bound)]

        def _between(_col_name):

            if invert is False and equal is False:
                op1 = operator.gt
                op2 = operator.lt
                opb = operator.__and__

            elif invert is False and equal is True:
                op1 = operator.ge
                op2 = operator.le
                opb = operator.__and__

            elif invert is True and equal is False:
                op1 = operator.lt
                op2 = operator.gt
                opb = operator.__or__

            elif invert is True and equal is True:
                op1 = operator.le
                op2 = operator.ge
                opb = operator.__or__

            sub_query = []
            for bound in bounds:
                _lower_bound, _upper_bound = bound
                sub_query.append(opb(op1(df[_col_name], _lower_bound), op2(df[_col_name], _upper_bound)))
            query = functools.reduce(operator.__or__, sub_query)

            return query

        # df = self
        for col_name in columns:
            df = df.rows.select(_between(col_name))
        df = df.meta.preserve(df, Actions.DROP_ROW.value, df.cols.names())
        return df

    def drop_by_dtypes(self, input_cols, data_type=None):
        df = self
        return df

    def drop_na(self, input_cols, how="any", *args, **kwargs):
        """
        Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
        'any' of the values is null.
        :param input_cols:
        :param how:
        :return:
        """
        df = self.df

        input_cols = val_to_list(input_cols)
        df = df.dropna(how=how, subset=input_cols, *args, **kwargs)
        return df

    def drop_duplicates(self, input_cols=None):
        """
        Drop duplicates values in a dataframe
        :param input_cols: List of columns to make the comparison, this only  will consider this subset of columns,
        :return: Return a new DataFrame with duplicate rows removed
        :param input_cols:
        :return:
        """
        df = self.df
        input_cols = parse_columns(df, input_cols)
        input_cols = val_to_list(input_cols)
        df = df.drop_duplicates(subset=input_cols)

        return df

    # def limit(self, count):
    #     """
    #     Limit the number of rows
    #     :param count:
    #     :return:
    #     """
    #     pass

    def is_in(self, input_cols, values):
        df = self.df
        return df

    def unnest(self, input_cols):
        df = self.df
        return df

    def approx_count(self):
        """
        Aprox rows count
        :return:
        """
        df = self.df
        return df.rows.count()
