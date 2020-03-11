import re
from enum import Enum

import numpy as np
import pandas as pd

from optimus.engines.base.columns import BaseColumns
from optimus.helpers.check import equal_function
from optimus.helpers.columns import parse_columns, get_output_cols, check_column_numbers
from optimus.helpers.core import val_to_list

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
            df = self
            # Std, kurtosis, mean, skewness and other agg functions can not process date columns.
            filters = {"object": [df.functions.min, df.functions.stddev],
                       }

            def _filter(_col_name, _func):
                for data_type, func_filter in filters.items():
                    for f in func_filter:
                        if equal_function(func, f) and \
                                df.cols.dtypes(_col_name)[_col_name] == data_type:
                            return True
                return False

            columns = parse_columns(df, columns)
            funcs = val_to_list(funcs)

            result = {}

            for func in funcs:
                # Create expression for functions that accepts multiple columns
                filtered_column = []
                for col_name in columns:
                    # If the key exist update it
                    if not _filter(col_name, func):
                        filtered_column.append(col_name)
                if len(filtered_column) > 0:
                    # print("AAA",func, args, df)
                    result = func(columns, args, df=df)

            return result

        @staticmethod
        def exec_agg(exprs):
            return exprs

        @staticmethod
        def lower(input_cols, output_cols=None):
            df = self
            input_cols = parse_columns(df, input_cols)
            # df.select_dtypes(exclude=['string', 'object'])

            output_cols = get_output_cols(input_cols, output_cols)
            for input_col, output_col in zip(input_cols, output_cols):
                if df[input_col].dtype == "object":
                    df[output_col] = df[input_col].str.lower()
            return df

        @staticmethod
        def upper(input_cols, output_cols=None):
            df = self
            input_cols = parse_columns(df, input_cols)

            output_cols = get_output_cols(input_cols, output_cols)
            for input_col, output_col in zip(input_cols, output_cols):
                if df[input_col].dtype == "object":
                    df[output_col] = df[input_col].str.upper()
            return df

        @staticmethod
        def trim(input_cols, output_cols=None):
            df = self
            input_cols = parse_columns(df, input_cols)

            output_cols = get_output_cols(input_cols, output_cols)
            for input_col, output_col in zip(input_cols, output_cols):
                if df[input_col].dtype == "object":
                    df[output_col] = df[input_col].str.strip()
            return df

        @staticmethod
        def reverse(input_cols, output_cols=None):
            pass

        @staticmethod
        def remove(columns, search=None, search_by="chars", output_cols=None):
            pass

        @staticmethod
        def remove_accents(input_cols, output_cols=None):
            df = self
            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)
            # cols = df.select_dtypes(include=[np.object]).columns

            for input_col, output_col in zip(input_cols,output_cols):
                if df[input_col].dtype == "object":
                    df[output_col] = df[input_col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            return df

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

            df = self
            input_cols = parse_columns(df, input_cols)
            search = val_to_list(search)
            if search_by == "chars":
                _regex = re.compile("|".join(map(re.escape, search)))
            elif search_by == "words":
                _regex = (r'\b%s\b' % r'\b|\b'.join(map(re.escape, search)))
            else:
                _regex = search

            for input_col in input_cols:
                if search_by == "chars" or search_by == "words":
                    df[input_col] = df[input_col].str.replace(_regex, replace_by)
                elif search_by == "full":
                    df[input_col] = df[input_col].replace(search, replace_by)

            return df

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
            df = self
            columns = parse_columns(df, columns)
            result = {}

            # from numba import njit
            # @njit
            def _count_na(_df, _serie):
                return np.count_nonzero(_df[_serie].isnull().values.ravel())

            for col_name in columns:
                # np is 2x faster than df[columns].isnull().sum().to_dict()
                # Reference https://stackoverflow.com/questions/28663856/how-to-count-the-occurrence-of-certain-item-in-an-ndarray-in-python
                result[col_name] = _count_na(df, col_name)
                # np.count_nonzero(df[col_name].isnull().values.ravel())
            return result

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
        def nunique_approx(columns):
            df = self
            return df.cols.nunique(columns)

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

            df = self
            if output_col is None:
                output_col = "_".join(input_cols)
            else:
                output_col = parse_columns(df, output_col, accepts_missing_cols=True)
            # print("ASSAD    ",output_col)
            # check_column_numbers(output_col, 1)

            import operator
            if shape == "string":
                df[output_col] = df[input_cols].agg(separator.join, axis=1)

            return df

        @staticmethod
        def unnest(input_cols, separator=None, splits=-1, index=None, output_cols=None, drop=False):
            df = self

            # new data frame with split value columns
            df_new = df[input_cols].str.split(separator, n=splits, expand=True)
            if splits == -1:
                splits = len(df_new.columns)
            # print(df_new)
            # print("asdf", len(df.columns))

            # Maybe the split do not generate new columns, We need to recalculate it
            num_columns = len(df_new.columns)
            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)

            # for idx, (input_col, output_col) in enumerate(zip(input_cols, output_cols)):

            for i in range(splits):
                # Making separate first name column from new data frame
                if i < num_columns:
                    df[output_cols[0] + "_" + str(i)] = df_new[i]
                else:
                    df[output_cols[0] + "_" + str(i)] = None

            # Dropping old Name columns
            if drop is True:
                df.drop(columns=input_cols, inplace=True)
            return df

        @staticmethod
        def find(columns, sub):
            """
            Find the position for a char or substring
            :param input_cols:
            :param sub:
            :return:
            """
            df = self
            columns = parse_columns(df, columns)

            def get_match_positions(_value, _separator):
                # print("AAAAAA", _value)
                length = [[match.start(), match.end()] for match in re.finditer(_separator, _value)]
                return length if len(length) > 0 else None

            for col_name in columns:
                df[col_name + "__match_positions__"] = df[col_name].apply(get_match_positions, args=sub)
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
            df = self
            result = {}
            df_len = len(df)
            for col_name, na in df.cols.count_na(columns).items():
                result[col_name] = {"no_missing": df_len - na, "missing": na, "mismatches": 0}
            return result

            # return np.count_nonzero(df.isnull().values.ravel())

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
        def nunique(columns):
            df = self
            columns = parse_columns(df, columns)
            result = {}
            # def _nunique(_df, _serie_name):
            #     return np.unique(_df[_serie_name].values.ravel())

            for col_name in columns:
                result[col_name] = df[col_name].nunique()

                # result[col_name] = _nunique(df,col_name)
            return result

        def count(self):
            df = self.df
            return len(df)

        @staticmethod
        def frequency(columns, n=10, percentage=False, total_rows=None):
            # https://stackoverflow.com/questions/10741346/numpy-most-efficient-frequency-counts-for-unique-values-in-an-array
            df = self
            columns = parse_columns(df, columns)

            result = {}
            for col_name in columns:

                if df[col_name].dtype == np.int64 or df[col_name].dtype == np.float64:
                    # i = np.bincount(df[col_name].to_numpy())
                    # j = np.argpartition(i, -4)[-4:]
                    # print("asdasdads",i)
                    # print("asdasdads",j)
                    # # j = np.nonzero(i)[0]
                    # result[col_name] = {"values": list(i), "count": list(j)}
                    i, j = np.unique(df[col_name], return_counts=True)
                    count_sort_ind = np.argsort(-j)
                    result[col_name] = {"values": list(i[count_sort_ind])[:n], "count": list(j[count_sort_ind])[:n]}
                # elif df[col_name].dtype == np.float64:
                #
                #     uniqw, inverse = np.unique(df[col_name],return_counts=True)
                #     i = np.bincount(inverse)
                #     j = np.nonzero(i)[0]
                #     result[col_name] = {"values": list(j[:n]), "count": list(i[j][:n])}

                else:
                    # print("frequency BBBBB", col_name)
                    # i, j = np.unique(df[col_name].to_numpy(), return_counts=True)
                    # count_sort_ind = np.argsort(-j)
                    # result[col_name] = {"values": list(i[count_sort_ind])[:n], "count": list(j[count_sort_ind])[:n]}

                    # Value counts
                    r = df[col_name].value_counts().nlargest(n)
                    i = r.index.tolist()
                    j = r.tolist()
                    # print("rrrrrrr",i)
                    result[col_name] = ({"values": i, "count": j})
            # print("RESULT",result)
            return result

    return Cols(self)


DataFrame.cols = property(cols)
