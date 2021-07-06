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
from optimus.infer import is_list_of_str_or_int, is_list_value
from optimus.engines.base.meta import Meta


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
            rows = cudf.DataFrame(rows)
        # Can not concatenate dataframe with not string columns names

        rows.columns = df.cols.names()
        # df = cudf.concat([df.reset_index(drop=True), rows.reset_index(drop=True)], axis=0)
        df = cudf.concat([df, rows], axis=0, ignore_index=True)
        return df

    def _sort(self, dfd, col_name, ascending):
        return dfd.sort_values(col_name, ascending=ascending)

    @staticmethod
    def unnest(input_cols) -> DataFrame:
        df = self.root
        return df
