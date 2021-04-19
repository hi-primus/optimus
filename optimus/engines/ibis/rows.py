import functools
import operator

import pandas as pd
from multipledispatch import dispatch

from optimus.engines.base.rows import BaseRows
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str_or_int, is_str, is_list_value
from optimus.engines.base.meta import Meta

DataFrame = pd.DataFrame


class Rows(BaseRows):
    def __init__(self, df):
        super(Rows, self).__init__(df)

    @staticmethod
    def create_id(column="id") -> DataFrame:
        pass

    def count(self, compute=True) -> int:
        """
        Count dataframe rows
        """
        dfd = self.root.data
        # TODO: Be sure that we need the compute param
        if compute is True:
            result = dfd.count().execute()
        else:
            result = dfd.count()
        return result

    def append(self, rows):
        """

        :param rows:
        :return:
        """
        df = self.df

        if is_list_value(rows):
            rows = pd.DataFrame(rows)
        # Can not concatenate dataframe with not string columns names

        rows.columns = df.cols.names()
        df = pd.concat([df.reset_index(drop=True), rows.reset_index(drop=True)], axis=0)
        return df

    def _sort(self, dfd, col_name, ascending):
        return dfd.sort_values(col_name, ascending=ascending)

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
        df.meta = Meta.action(df.meta, None, Actions.DROP_ROW.value, df.cols.names())
        return df

    @staticmethod
    def drop_by_dtypes(input_cols, data_type=None):
        df = self
        return df

    @staticmethod
    def tag_nulls(how="all", subset=None, output_col=None):
        """
        Find the rows that have null values
        :param how:
        :param subset:
        :param output_col:
        :return:
        """

        df = self

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

        return df

    @staticmethod
    def tag_duplicated(keep="first", subset=None, output_col=None):
        """
        Find the rows that have null values

        :param keep:
        :param output_col:
        :return:
        """

        df = self
        if subset is not None:
            subset = val_to_list(subset)
            subset_df = df[subset]
        else:
            subset_df = df

        if output_col is None:
            output_col = "__duplicated__"

        df[output_col] = subset_df.duplicated(keep=keep, subset=subset)

        return df

    @staticmethod
    def drop_duplicates(subset=None) -> DataFrame:
        """
        Drop duplicates values in a dataframe
        :param subset: List of columns to make the comparison, this only  will consider this subset of columns,
        :return: Return a new DataFrame with duplicate rows removed
        :return:
        """
        df = self
        subset = parse_columns(df, subset)
        subset = val_to_list(subset)
        df = df.drop_duplicates(subset=subset)

        return df

    @staticmethod
    def unnest(input_cols) -> DataFrame:
        df = self
        return df
