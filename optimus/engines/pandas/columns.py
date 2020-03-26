import re

import numpy as np
import pandas as pd

from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.engines.jit import min_max, bincount
from optimus.engines.pandas.ml.encoding import string_to_index as ml_string_to_index
from optimus.helpers.check import equal_function
from optimus.helpers.columns import parse_columns, get_output_cols, check_column_numbers
from optimus.helpers.core import val_to_list

DataFrame = pd.DataFrame


def cols(self: DataFrame):
    class Cols(DataFrameBaseColumns):
        def __init__(self, df):
            super(DataFrameBaseColumns, self).__init__(df)

        @staticmethod
        def append(*args, **kwargs):
            pass

        @staticmethod
        def to_timestamp(input_cols, date_format=None, output_cols=None):
            pass

        @staticmethod
        def apply_expr(input_cols, func=None, args=None, filter_col_by_dtypes=None, output_cols=None, meta=None):
            pass

        def apply(self, input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
                  filter_col_by_dtypes=None, output_cols=None, skip_output_cols_processing=False, meta="apply"):
            df = self.df

            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)

            # for input_col, output_col in zip(input_cols, output_cols):
            def func(value, _func, _args):
                # print(input_cols, output_cols),.
                # print(value)
                # for input_col, output_col in zip(input_cols, _output_cols):
                # print("output_co", output_col)
                # print("input_col", input_col)
                # row[output_cols[0]] = row[input_cols[0]] + 1
                return _func(value, _args)

            # apply numpy vectorization
            for input_col, output_col in zip(input_cols, output_cols):
                df[output_col] = df[input_cols].apply(func, args=(func, args))

            return df

        @staticmethod
        def apply_by_dtypes(columns, func, func_return_type, args=None, func_type=None, data_type=None):
            pass

        @staticmethod
        def set(output_col, value=None):
            pass

        # @staticmethod
        # def cast(input_cols=None, dtype=None, output_cols=None, columns=None):
        #     df = self
        #     input_cols = parse_columns(df, input_cols)
        #     df[input_cols] = df[input_cols].astype(dtype)
        #
        #     return df

        @staticmethod
        def astype(*args, **kwargs):
            pass

        @staticmethod
        def move(column, position, ref_col=None):
            pass

        def mode(self, columns):
            df = self.df
            columns = parse_columns(df, columns)
            result = {}
            for col_name in columns:
                result[col_name] = df[col_name].mode(col_name)[0]
            return result

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
                    result = func(columns, args, df=df)

            return result

        @staticmethod
        def replace(input_cols, search=None, replace_by=None, search_by="chars", output_cols=None):
            df = self
            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)
            search = val_to_list(search)
            if search_by == "chars":
                _regex = re.compile("|".join(map(re.escape, search)))
            elif search_by == "words":
                _regex = (r'\b%s\b' % r'\b|\b'.join(map(re.escape, search)))
            else:
                _regex = search

            df = df.cols.cast(input_cols, "str")
            for input_col, output_col in zip(input_cols, output_cols):
                if search_by == "chars" or search_by == "words":
                    df[output_col] = df[input_col].str.replace(_regex, replace_by)
                elif search_by == "full":
                    df[output_col] = df[input_col].replace(search, replace_by)

            return df

        @staticmethod
        def exec_agg(exprs):
            return exprs

        @staticmethod
        def remove(columns, search=None, search_by="chars", output_cols=None):
            pass

        @staticmethod
        def remove_accents(input_cols, output_cols=None):
            df = self
            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)
            # cols = df.select_dtypes(include=[np.object]).columns

            for input_col, output_col in zip(input_cols, output_cols):
                if df[input_col].dtype == "object":
                    df[output_col] = df[input_col].str.normalize('NFKD').str.encode('ascii',
                                                                                    errors='ignore').str.decode('utf-8')
            return df

        @staticmethod
        def date_transform(input_cols, current_format=None, output_format=None, output_cols=None):
            pass

        @staticmethod
        def years_between(input_cols, date_format=None, output_cols=None):
            pass

        @staticmethod
        def replace_regex(input_cols, regex=None, value=None, output_cols=None):
            pass

        @staticmethod
        def impute(input_cols, data_type="continuous", strategy="mean", output_cols=None):
            pass

        @staticmethod
        def is_na(input_cols, output_cols=None):
            pass

        @staticmethod
        def extract(input_cols, output_cols, regex):
            df = self
            from optimus.engines.base.dataframe.commons import extract
            df = extract(df, input_cols, output_cols, regex)
            return df

        @staticmethod
        def min_max(columns):
            """
            Calculate min max in one pass.
            :param columns:
            :return:
            """

            df = self
            columns = parse_columns(df, columns)
            result = {}
            for col_name in columns:
                _min, _max = min_max(df[col_name].to_numpy())

                result[col_name] = {"min": _min, "max": _max}
            return result

        @staticmethod
        def count_na(columns):
            df = self
            columns = parse_columns(df, columns)
            result = {}

            def _count_na(_df, _serie):
                return np.count_nonzero(_df[_serie].isnull().values.ravel())

            for col_name in columns:
                # np is 2x faster than df[columns].isnull().sum().to_dict()
                # Reference https://stackoverflow.com/questions/28663856/how-to-count-the-occurrence-of-certain-item-in-an-ndarray-in-python
                result[col_name] = _count_na(df, col_name)
            return result

        @staticmethod
        def count_zeros(columns):
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
        def min_max_scaler(input_cols, output_cols=None):
            pass

        @staticmethod
        def standard_scaler(input_cols, output_cols=None):
            pass

        @staticmethod
        def max_abs_scaler(input_cols, output_cols=None):
            pass

        @staticmethod
        def find(columns, sub):
            """
            Find the position for a char or substring
            :param columns:
            :param sub:
            :return:
            """
            df = self
            columns = parse_columns(df, columns)

            def get_match_positions(_value, _separator):
                result = None
                if _value is not np.nan:
                    length = [[match.start(), match.end()] for match in re.finditer(_separator, _value)]
                    result = length if len(length) > 0 else None
                return result

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
        def qcut(columns, num_buckets, handle_invalid="skip"):
            pass

        @staticmethod
        def clip(columns, lower_bound, upper_bound):
            pass

        @staticmethod
        def values_to_cols(input_cols):
            pass

        def string_to_index(input_cols=None, output_cols=None, columns=None):
            df = self
            print("asdfajkhslkjdhf")
            df = ml_string_to_index(df, input_cols, output_cols, columns)

            return df

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
                    i, j = bincount(df[col_name], n)

                    result[col_name] = {"values": list(i), "count": list(j)}
                else:
                    # Value counts
                    r = df[col_name].value_counts().nlargest(n)
                    i = r.index.tolist()
                    j = r.tolist()
                    result[col_name] = ({"values": i, "count": j})
            return result

    return Cols(self)


DataFrame.cols = property(cols)
