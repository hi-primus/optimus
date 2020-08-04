import functools
import operator

import cudf
import pandas as pd
from cudf.core import DataFrame
from multipledispatch import dispatch

from optimus.engines.base.rows import BaseRows
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str_or_int, is_list


def rows(self):
    class Rows(BaseRows):
        def __init__(self, df):
            super(Rows, self).__init__(df)

        @staticmethod
        def create_id(column="id") -> DataFrame:
            pass

        def append(self, rows):
            """

            :param rows:
            :return:
            """
            df = self.df

            if is_list(rows):
                rows = cudf.DataFrame(rows)
            # Can not concatenate dataframe with not string columns names

            rows.columns = df.cols.names()
            # df = cudf.concat([df.reset_index(drop=True), rows.reset_index(drop=True)], axis=0)
            df = cudf.concat([df, rows], axis=0, ignore_index=True)
            return df


        @staticmethod
        def to_list(input_cols):
            """

            :param input_cols:
            :return:
            """
            input_cols = parse_columns(self, input_cols)
            df_list = self[input_cols].to_pandas().values.tolist()

            return df_list

        @staticmethod
        @dispatch(str, str)
        def sort(input_cols) -> DataFrame:
            input_cols = parse_columns(self, input_cols)
            return self.rows.sort([(input_cols, "desc",)])

        @staticmethod
        @dispatch(str, str)
        def sort(columns, order="desc") -> DataFrame:
            """
            Sort column by row
            """
            columns = parse_columns(self, columns)
            return self.rows.sort([(columns, order,)])

        @staticmethod
        @dispatch(list)
        def sort(col_sort) -> DataFrame:
            """
            Sort rows taking into account multiple columns
            :param col_sort: column and sort type combination (col_name, "asc")
            :type col_sort: list of tuples
            """
            # If a list of columns names are given order this by desc. If you need to specify the order of every
            # column use a list of tuples (col_name, "asc")
            df = self

            t = []
            if is_list_of_str_or_int(col_sort):
                for col_name in col_sort:
                    t.append(tuple([col_name, "desc"]))
                col_sort = t

            for cs in col_sort:
                col_name = one_list_to_val(cs[0])
                order = cs[1]

                if order != "asc" and order != "desc":
                    RaiseIt.value_error(order, ["asc", "desc"])

                df = df.meta.preserve(self, Actions.SORT_ROW.value, col_name)

                c = df.cols.names()
                # It seems that is on posible to order rows in Dask using set_index. It only return data in ascendent way.
                # We should fins a way to make it work desc and form multiple columns
                df.set_index(col_name).reset_index()[c].head()

            return df

        @staticmethod
        def between(columns, lower_bound=None, upper_bound=None, invert=False, equal=False,
                    bounds=None) -> DataFrame:
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
            # TODO: should process string or dates
            columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
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

            df = self
            for col_name in columns:
                df = df.rows.select(_between(col_name))
            df = df.meta.preserve(self, Actions.DROP_ROW.value, df.cols.names())
            return df

        @staticmethod
        def drop_by_dtypes(input_cols, data_type=None):
            df = self
            return df

        @staticmethod
        def drop_duplicates(input_cols=None) -> DataFrame:
            """
            Drop duplicates values in a dataframe
            :param input_cols: List of columns to make the comparison, this only  will consider this subset of columns,
            :return: Return a new DataFrame with duplicate rows removed
            :param input_cols:
            :return:
            """
            df = self
            input_cols = parse_columns(df, input_cols)
            input_cols = val_to_list(input_cols)
            df = df.drop_duplicates(subset=input_cols)

            return df

        @staticmethod
        def limit(count) -> DataFrame:
            """
            Limit the number of rows
            :param count:
            :return:
            """

            return self[:count - 1]

        @staticmethod
        def is_in(input_cols, values) -> DataFrame:
            df = self
            return df

        @staticmethod
        def unnest(input_cols) -> DataFrame:
            df = self
            return df

    return Rows(self)


DataFrame.rows = property(rows)
