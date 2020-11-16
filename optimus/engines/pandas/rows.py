import functools
import operator

import pandas as pd
from multipledispatch import dispatch

from optimus.engines.base.rows import BaseRows
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str_or_int, is_list

DataFrame = pd.DataFrame


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
        df = self.parent

        if is_list(rows):
            rows = pd.DataFrame(rows)
        # Can not concatenate dataframe with not string columns names

        rows.columns = df.cols.names()
        df = pd.concat([df.reset_index(drop=True), rows.reset_index(drop=True)], axis=0)
        return self.parent.new(df)

    def to_list(self, input_cols):
        """

        :param input_cols:
        :return:
        """
        df = self.parent
        input_cols = parse_columns(df.data, input_cols)
        df_list = df[input_cols].values.tolist()

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


    @dispatch(list)
    def sort(self, col_sort) -> DataFrame:
        """
        Sort rows taking into account multiple columns
        :param col_sort: column and sort type combination (col_name, "asc")
        :type col_sort: list of tuples
        """
        # If a list of columns names are given order this by desc. If you need to specify the order of every
        # column use a list of tuples (col_name, "asc")
        df = self.parent

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

            df = df.meta.preserve(Actions.SORT_ROW.value, col_name)

            df = df.sort_values(col_name, ascending=True if order == "asc" else False)

        return df

    def between(self, columns, lower_bound=None, upper_bound=None, invert=False, equal=False,
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
        df = self.parent
        # TODO: should process string or dates
        columns = parse_columns(df.data, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
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
        df = df.meta.preserve(self, Actions.DROP_ROW.value, df.cols.names())
        return self.parent.new(df)

    @staticmethod
    def drop_by_dtypes(input_cols, data_type=None):
        df = self
        return df

    def tag_nulls(self, how="all", subset=None, output_col=None):
        """
        Find the rows that have null values
        :param how:
        :param subset:
        :param output_col:
        :return:
        """

        df = self.parent.data

        if subset is not None:
            subset = val_to_list(subset)
            subset_df = df[subset]
        else:
            subset_df = df

        if output_col is None:
            output_col = "__nulls__"

        if how == "all":
            df[output_col] = subset_df.isnull().all(axis=1)
        else:
            df[output_col] = subset_df.isnull().any(axis=1)

        return self.parent.new(df)

    def tag_duplicated(self, keep="first", subset=None, output_col=None):
        """
        Find the rows that have null values

        :param keep:
        :param subset:
        :param output_col:
        :return:
        """

        df = self.parent.data
        if subset is not None:
            subset = val_to_list(subset)
            subset_df = df[subset]
        else:
            subset_df = df

        if output_col is None:
            output_col = "__duplicated__"

        df[output_col] = subset_df.duplicated(keep=keep, subset=subset)

        return self.parent.new(df)

    def drop_duplicates(self, subset=None) -> DataFrame:
        """
        Drop duplicates values in a dataframe
        :param subset: List of columns to make the comparison, this only  will consider this subset of columns,
        :return: Return a new DataFrame with duplicate rows removed
        :return:
        """
        df = self.parent.data
        subset = parse_columns(df, subset)
        subset = val_to_list(subset)
        df = df.drop_duplicates(subset=subset)

        return self.parent.new(df)

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
