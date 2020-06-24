import re

import numpy as np
import pandas as pd
from sklearn.preprocessing import KBinsDiscretizer

from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.engines.jit import min_max, bincount
from optimus.engines.pandas.ml.encoding import index_to_string as ml_index_to_string
from optimus.engines.pandas.ml.encoding import string_to_index as ml_string_to_index
from optimus.helpers.columns import parse_columns, get_output_cols, prepare_columns
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.functions import set_function_parser, set_func
from optimus.infer import is_str

DataFrame = pd.DataFrame


def cols(self: DataFrame):
    class Cols(DataFrameBaseColumns):
        def __init__(self, df):
            super(DataFrameBaseColumns, self).__init__(df)


        @staticmethod
        def to_timestamp(input_cols, date_format=None, output_cols=None):
            pass

        def set(self, where=None, value=None, output_cols=None, default=None):

            df = self.df

            columns, vfunc = set_function_parser(df, value, where, default)
            # if df.cols.dtypes(input_col) == "category":
            #     try:
            #         # Handle error if the category already exist
            #         df[input_col] = df[input_col].cat.add_categories(val_to_list(value))
            #     except ValueError:
            #         pass

            output_cols = one_list_to_val(output_cols)

            if columns:
                final_value = set_func(df[columns], value=value, where=where, output_col=output_cols, parser=vfunc,
                                       default=default)
            else:
                final_value = set_func(df, value=value, where=where, output_col=output_cols, parser=vfunc,
                                       default=default)

            kw_columns = {output_cols: final_value}
            return df.assign(**kw_columns)

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
        def replace(input_cols, search=None, replace_by=None, search_by="chars", ignore_case=False, output_cols=None):
            df = self
            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)
            # If tupple

            search = val_to_list(search)

            if search_by == "chars":
                str_regex = "|".join(map(re.escape, search))
            elif search_by == "words":
                str_regex = (r'\b%s\b' % r'\b|\b'.join(map(re.escape, search)))
            else:
                str_regex = search
            if ignore_case is True:
                _regex = re.compile(str_regex, re.IGNORECASE)
            else:
                _regex = re.compile(str_regex)

            # df = df.cols.cast(input_cols, "str")
            for input_col, output_col in zip(input_cols, output_cols):
                if search_by == "chars" or search_by == "words":
                    df[output_col] = df[input_col].astype(str).str.replace(_regex, replace_by)
                elif search_by == "full":
                    df[output_col] = df[input_col].astype(str).replace(search, replace_by)

            return df

        @staticmethod
        def exec_agg(exprs):
            return exprs

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

        def weekday(self, input_cols, output_cols=None):
            pass

        def weekofyear(self, input_cols, output_cols=None):
            pass

        # NLP
        @staticmethod
        def stem_words(input_col):
            df = self

        @staticmethod
        def lemmatize_verbs(input_cols, output_cols=None):
            df = self

            def func(value, args=None):
                return value + "aaa"

            df = df.cols.apply(input_cols, func, output_cols)
            return df

        def remove_stopwords(self):
            df = self

        def strip_html(self):
            df = self
            # soup = BeautifulSoup(self.text, "html.parser")
            # self.text = soup.get_text()
            return self

        # @staticmethod
        # def mismatches_1(columns, dtype):
        #     """
        #     Find the rows that have null values
        #     :param dtype:
        #     :param columns:
        #     :return:
        #     """
        #     df = self
        #     columns = parse_columns(df, columns)
        #
        #     from optimus.infer import is_bool, is_list
        #
        #     def func(d_type):
        #         if d_type == "bool":
        #             return is_bool
        #         elif d_type == "int":
        #             return fastnumbers.isint
        #         elif d_type == "float":
        #             return fastnumbers.isfloat
        #         elif d_type == "list":
        #             return is_list
        #         elif d_type == "str":
        #             return None
        #         elif d_type == "object":
        #             return None
        #
        #     f = func(dtype)
        #     if f is None:
        #         for col_name in columns:
        #             # df[col_name + "__match_positions__"] = df[col_name].apply(get_match_positions, args=sub)
        #             df = df[df[col_name].apply(f)]
        #         return df

        @staticmethod
        def find(columns, sub, ignore_case=False):
            """
            Find the start and end position for a char or substring
            :param columns:
            :param ignore_case:
            :param sub:
            :return:
            """
            df = self
            columns = parse_columns(df, columns)
            sub = val_to_list(sub)

            def get_match_positions(_value, _separator):

                result = None
                if is_str(_value):
                    # Using re.IGNORECASE in finditer not seems to work
                    if ignore_case is True:
                        _separator = _separator + [s.lower() for s in _separator]
                    regex = re.compile('|'.join(_separator))

                    length = [[match.start(), match.end()] for match in
                              regex.finditer(_value)]
                    result = length if len(length) > 0 else None
                return result

            for col_name in columns:
                # Categorical columns can not handle a list inside a list as return for example [[1,2],[6,7]].
                # That could happened if we try to split a categorical column
                # df[col_name] = df[col_name].astype("object")
                df[col_name + "__match_positions__"] = df[col_name].astype("object").apply(get_match_positions,
                                                                                           args=(sub,))
            return df

        @staticmethod
        def scatter(columns, buckets=10):
            pass

        @staticmethod
        def count_mismatch(columns_mismatch: dict = None):
            pass

        @staticmethod
        def count_by_dtypes(columns, dtype):

            df = self
            result = {}
            df_len = len(df)
            for col_name, na_count in df.cols.count_na(columns).items():
                # for i, j in df.constants.DTYPES_DICT.items():
                #     if j == df[col_name].dtype.type:
                #         _dtype = df.constants.SHORT_DTYPES[i]

                # _dtype = df.cols.dtypes(col_name)[col_name]

                mismatches_count = df.cols.is_match(col_name, dtype).value_counts().to_dict().get(False)
                mismatches_count = 0 if mismatches_count is None else mismatches_count
                result[col_name] = {"match": df_len - na_count, "missing": na_count,
                                    "mismatch": mismatches_count - na_count}
            return result

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
        def string_to_index(input_cols=None, output_cols=None, columns=None):
            df = self
            df = ml_string_to_index(df, input_cols, output_cols, columns)

            return df

        @staticmethod
        def index_to_string(input_cols=None, output_cols=None, columns=None):
            df = self
            df = ml_index_to_string(df, input_cols, output_cols, columns)

            return df

        @staticmethod
        def bucketizer(input_cols, splits, output_cols=None):
            df = self

            columns = prepare_columns(df, input_cols, output_cols, merge=True)

            for input_col, output_col in columns:
                x = df[[input_col]]
                est = KBinsDiscretizer(n_bins=splits, encode='ordinal', strategy='uniform')
                est.fit(input_col)
                df[output_col] = est.transform(x)
            return df


        @staticmethod
        def frequency(columns, n=10, percentage=False, total_rows=None):
            # https://stackoverflow.com/questions/10741346/numpy-most-efficient-frequency-counts-for-unique-values-in-an-array
            df = self
            columns = parse_columns(df, columns)

            result = {}
            for col_name in columns:
                if df[col_name].dtype == np.int64 or df[col_name].dtype == np.float64:
                    i, j = bincount(df[col_name], n)
                else:
                    # Value counts
                    r = df[col_name].value_counts().nlargest(n)
                    i = r.index.tolist()
                    j = r.tolist()
                col_values = [{"value": value, "count": count} for value, count in zip(i, j)]

                result[col_name] = {"frequency": col_values}
            return result

    return Cols(self)


DataFrame.cols = property(cols)
