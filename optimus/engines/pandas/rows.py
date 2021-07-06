import functools
import operator

import pandas as pd
from multipledispatch import dispatch

from optimus.engines.base.meta import Meta
from optimus.engines.base.rows import BaseRows
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str_or_int, is_list_value

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
        df = self.root

        if is_list_value(rows):
            rows = self.root.new(pd.DataFrame(rows))
        # Can not concatenate dataframe with not string columns names

        rows.columns = df.cols.names()
        df.data.reset_index(drop=True)
        df = pd.concat([df.data.reset_index(drop=True), rows.data.reset_index(drop=True)], axis=0)
        return self.root.new(df)

    def _sort(self, dfd, col_name, ascending):
        return dfd.sort_values(col_name, ascending=ascending)

    def drop_duplicates(self, subset=None) -> DataFrame:
        """
        Drop duplicates values in a dataframe
        :param subset: List of columns to make the comparison, this only  will consider this subset of columns,
        :return: Return a new DataFrame with duplicate rows removed
        :return:
        """
        df = self.root
        dfd = df.data
        subset = parse_columns(df, subset)
        subset = val_to_list(subset)
        dfd = dfd.drop_duplicates(subset=subset)

        return self.root.new(dfd)



    @staticmethod
    def unnest(input_cols) -> DataFrame:
        df = self
        return df
