from enum import Enum

import numpy as np
import pandas as pd

from optimus.engines.base.columns import BaseColumns
from optimus.helpers.columns import parse_columns, get_output_cols, check_column_numbers

DataFrame = pd.DataFrame


def cols(self: DataFrame):
    class Cols(BaseColumns):
        @staticmethod
        def append(*args, **kwargs):
            pass

        @staticmethod
        def select(columns="*", regex=None, data_type=None, invert=False) -> str:
            df = self
            columns = parse_columns(df, columns, is_regex=regex, filter_by_column_dtypes=data_type, invert=invert)
            if columns is not None:
                df = df[columns]
                # Metadata get lost when using select(). So we copy here again.
                df = df.meta.preserve(self)

            else:
                df = None
            # print(type(df))
            return df

        @staticmethod
        def copy(input_cols, output_cols=None, columns=None):
            pass

        @staticmethod
        def to_timestamp(input_cols, date_format=None, output_cols=None):
            pass

        @staticmethod
        def apply_expr(input_cols, func=None, args=None, filter_col_by_dtypes=None, output_cols=None, meta=None):
            pass

        @staticmethod
        def apply(input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
                  filter_col_by_dtypes=None, output_cols=None, skip_output_cols_processing=False, meta="apply"):
            pass

        @staticmethod
        def apply_by_dtypes(columns, func, func_return_type, args=None, func_type=None, data_type=None):
            pass

        @staticmethod
        def set(output_col, value=None):
            pass

        @staticmethod
        def rename(*args, **kwargs) -> Enum:
            pass

        @staticmethod
        def cast(input_cols=None, dtype=None, output_cols=None, columns=None):
            pass

        @staticmethod
        def astype(*args, **kwargs):
            pass

        @staticmethod
        def move(column, position, ref_col=None):
            pass

        @staticmethod
        def keep(columns=None, regex=None):
            pass

        @staticmethod
        def sort(order="asc", columns=None):
            pass

        @staticmethod
        def drop(columns=None, regex=None, data_type=None):
            pass

        @staticmethod
        def create_exprs(columns, funcs, *args):
            pass

        @staticmethod
        def exec_agg(exprs):
            pass

        @staticmethod
        def lower(input_cols, output_cols=None):
            pass

        @staticmethod
        def upper(input_cols, output_cols=None):
            pass

        @staticmethod
        def trim(input_cols, output_cols=None):
            pass

        @staticmethod
        def reverse(input_cols, output_cols=None):
            pass

        @staticmethod
        def remove(columns, search=None, search_by="chars", output_cols=None):
            pass

        @staticmethod
        def remove_accents(input_cols, output_cols=None):
            pass

        @staticmethod
        def remove_special_chars(input_cols, output_cols=None):
            pass

        @staticmethod
        def remove_white_spaces(input_cols, output_cols=None):
            pass

        @staticmethod
        def date_transform(input_cols, current_format=None, output_format=None, output_cols=None):
            pass

        @staticmethod
        def years_between(input_cols, date_format=None, output_cols=None):
            pass

        @staticmethod
        def replace(input_cols, search=None, replace_by=None, search_by="chars", output_cols=None):
            pass

        @staticmethod
        def replace_regex(input_cols, regex=None, value=None, output_cols=None):
            pass

        @staticmethod
        def impute(input_cols, data_type="continuous", strategy="mean", output_cols=None):
            pass

        @staticmethod
        def fill_na(input_cols, value=None, output_cols=None):
            pass

        @staticmethod
        def is_na(input_cols, output_cols=None):
            pass

        @staticmethod
        def count_na(columns):
            pass

        @staticmethod
        def count_zeros(columns):
            pass

        @staticmethod
        def count_uniques(columns, estimate=True):
            pass

        @staticmethod
        def value_counts(columns):
            pass

        @staticmethod
        def unique(columns):
            pass

        @staticmethod
        def nunique(*args, **kwargs):
            pass

        @staticmethod
        def select_by_dtypes(data_type):
            pass

        @staticmethod
        def _math(columns, operator, new_column):
            pass

        @staticmethod
        def z_score(input_cols, output_cols=None):
            pass

        @staticmethod
        def min_max_scaler(input_cols, output_cols=None):
            pass

        @staticmethod
        def standard_scaler(input_cols, output_cols=None):
            pass

        @staticmethod
        def max_abs_scaler(input_cols, output_cols=None):
            pass

        @staticmethod
        def iqr(columns, more=None, relative_error=None):
            pass

        @staticmethod
        def nest(input_cols, shape="string", separator="", output_col=None):
            pass

        @staticmethod
        def unnest(input_cols, separator=None, splits=None, index=None, output_cols=None, drop=False):
            df = self

            # new data frame with split value columns
            new = df[input_cols].str.split(separator, n=splits, expand=True)

            for i in range(len(new.columns)-1):
                # making separate first name column from new data frame
                df["new name" + str(i)] = new[i]

            # Dropping old Name columns
            if drop is True:
                df.drop(columns=[input_cols], inplace=True)

            
            return df

        @staticmethod
        def cell(column):
            pass

        @staticmethod
        def scatter(columns, buckets=10):
            pass

        @staticmethod
        def frequency_by_group(columns, n=10, percentage=False, total_rows=None):
            pass

        @staticmethod
        def count_mismatch(columns_mismatch: dict = None):
            pass

        @staticmethod
        def count_by_dtypes(columns, infer=False, str_funcs=None, int_funcs=None):
            pass

        @staticmethod
        def correlation(input_cols, method="pearson", output="json"):
            pass

        @staticmethod
        def boxplot(columns):
            pass

        @staticmethod
        def schema_dtype(columns="*"):
            pass

        @staticmethod
        def qcut(columns, num_buckets, handle_invalid="skip"):
            pass

        @staticmethod
        def clip(columns, lower_bound, upper_bound):
            pass

        @staticmethod
        def values_to_cols(input_cols):
            pass

        @staticmethod
        def string_to_index(input_cols=None, output_cols=None, columns=None):
            pass

        @staticmethod
        def index_to_string(input_cols=None, output_cols=None, columns=None):
            pass

        @staticmethod
        def bucketizer(input_cols, splits, output_cols=None):
            pass

        @staticmethod
        def set_meta(col_name, spec=None, value=None, missing=dict):
            pass

        @staticmethod
        def get_meta(col_name, spec=None):
            pass

        @staticmethod
        def abs(input_cols, output_cols=None):
            """
            Apply abs to the values in a column
            :param input_cols:
            :param output_cols:
            :return:
            """
            df = self
            input_cols = parse_columns(df, input_cols, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
            output_cols = get_output_cols(input_cols, output_cols)

            check_column_numbers(output_cols, "*")
            # Abs not accepts column's string names. Convert to Spark Column

            # TODO: make this in one pass.

            for input_col, output_col in zip(input_cols, output_cols):
                df[output_col] = df.compute(np.abs(df[input_col]))
            return df

        @staticmethod
        def mode(columns):
            df = self
            return df

        @staticmethod
        def frequency(columns, n=10, percentage=False, total_rows=None):
            return False

    return Cols(self)


DataFrame.cols = property(cols)
