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
    def unnest(input_cols) -> DataFrame:
        df = self
        return df
