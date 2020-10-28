import re

import pandas as pd
from sklearn import preprocessing

from optimus.engines.base.commons.functions import to_integer, to_float, impute, string_to_index, index_to_string
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list
from optimus.infer import is_str

from ibis.expr.types import TableExpr

DataFrame = TableExpr


def cols(self: DataFrame):
    class Cols(DataFrameBaseColumns):
        def __init__(self, df):
            super(DataFrameBaseColumns, self).__init__(df)

        def append(self, dfs):
            """

            :param dfs:
            :return:
            """

            df = self.df
            df = pd.concat([dfs.reset_index(drop=True), df.reset_index(drop=True)], axis=1)
            return df

        @staticmethod
        def to_timestamp(input_cols, date_format=None, output_cols=None):
            pass

        def to_string(self, input_cols, output_cols=None):
            df = self.df

            return df.cols.apply(input_cols, str, output_cols=output_cols, meta_action=Actions.TO_STRING.value,
                                 mode="map")

        def to_integer(self, input_cols, output_cols=None):

            df = self.df

            return df.cols.apply(input_cols, to_integer, output_cols=output_cols, meta_action=Actions.TO_INTEGER.value,
                                 mode="map")

        def to_float(self, input_cols="*", output_cols=None):
            df = self.df

            return df.cols.apply(input_cols, to_float, output_cols=output_cols, meta_action=Actions.TO_FLOAT.value,
                                 mode="map")

        def impute(self, input_cols, data_type="continuous", strategy="mean", output_cols=None):
            df = self.df
            return impute(df, input_cols, data_type="continuous", strategy="mean", output_cols=None)

        @staticmethod
        def astype(*args, **kwargs):
            pass

        # NLP
        # @staticmethod
        # def stem_words(input_col):
        #     df = self
        #
        # @staticmethod
        # def lemmatize_verbs(input_cols, output_cols=None):
        #     df = self
        #
        #     def func(value, args=None):
        #         return value + "aaa"
        #
        #     df = df.cols.apply(input_cols, func, output_cols)
        #     return df
        #
        # def remove_stopwords(self):
        #     df = self
        #
        # def strip_html(self):
        #     df = self
        #     # soup = BeautifulSoup(self.text, "html.parser")
        #     # self.text = soup.get_text()
        #     return self


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
        def count_by_dtypes(columns, dtype):

            df = self
            result = {}
            df_len = len(df)
            for col_name, na_count in df.cols.count_na(columns, tidy=False)["count_na"].items():
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
        def qcut(columns, num_buckets, handle_invalid="skip"):
            pass

        def string_to_index(self, input_cols=None, output_cols=None, columns=None):
            df = self.df
            le = preprocessing.LabelEncoder()
            df = string_to_index(df, input_cols, output_cols, le)

            return df

        def index_to_string(self, input_cols=None, output_cols=None, columns=None):
            df = self.df
            le = preprocessing.LabelEncoder()
            df = index_to_string(df, input_cols, output_cols, le)

            return df

    return Cols(self)


DataFrame.cols = property(cols)
