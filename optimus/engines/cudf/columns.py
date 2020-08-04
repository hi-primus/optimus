import re

import cudf
from cudf.core import DataFrame
from cuml import preprocessing
from sklearn.preprocessing import StandardScaler

from optimus.engines.base.commons.functions import to_integer_cudf, to_float_cudf, string_to_index, index_to_string
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.helpers.columns import parse_columns, get_output_cols
from optimus.helpers.constants import Actions


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
            df = cudf.concat([dfs.reset_index(drop=True), df.reset_index(drop=True)], axis=1)
            return df

        def to_integer(self, input_cols, output_cols=None):

            df = self.df
            return df.cols.apply(input_cols, to_integer_cudf, output_cols=output_cols,
                                 meta_action=Actions.TO_FLOAT.value,
                                 mode="pandas")

        def to_float(self, input_cols="*", output_cols=None):

            df = self.df

            return df.cols.apply(input_cols, to_float_cudf, output_cols=output_cols, meta_action=Actions.TO_FLOAT.value,
                                 mode="pandas")

        @staticmethod
        def to_timestamp(input_cols, date_format=None, output_cols=None):
            pass

        def reverse(self, input_cols, output_cols=None):
            raise NotImplementedError('Not implemented yet')

        # https://github.com/rapidsai/cudf/issues/3177
        # def replace(self, input_cols, search=None, replace_by=None, search_by="chars", output_cols=None):
        #     """
        #     Replace a value, list of values by a specified string
        #     :param input_cols: '*', list of columns names or a single column name.
        #     :param output_cols:
        #     :param search: Values to look at to be replaced
        #     :param replace_by: New value to replace the old one
        #     :param search_by: Can be "full","words","chars" or "numeric".
        #     :return: Dask DataFrame
        #     """
        #
        #     df = self.df
        #     columns = prepare_columns(df, input_cols, output_cols)
        #
        #     search = val_to_list(search)
        #     if search_by == "chars":
        #         _regex = search
        #         # _regex = re.compile("|".join(map(re.escape, search)))
        #     elif search_by == "words":
        #         _regex = (r'\b%s\b' % r'\b|\b'.join(map(re.escape, search)))
        #     else:
        #         _regex = search
        #
        #     if not is_str(replace_by):
        #         RaiseIt.type_error(replace_by, ["str"])
        #
        #     kw_columns = {}
        #     for input_col, output_col in columns:
        #         if search_by == "chars" or search_by == "words":
        #             # This is only implemented in cudf
        #             kw_columns[output_col] = df[input_col].astype(str).str.replace_multi(search, replace_by,
        #                                                                                  regex=False)
        #         elif search_by == "full":
        #             kw_columns[output_col] = df[input_col].astype(str).replace(search, replace_by)
        #
        #     print("kw_columns",kw_columns)
        #     print("search",search)
        #     df.assign(**kw_columns)
        #     return df

        def replace_regex(self, input_cols, regex=None, replace_by=None, output_cols=None):
            df = self.df
            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)
            for input_col, output_col in zip(input_cols, output_cols):
                df[output_col] = df[input_col].str.replace_multi(regex, replace_by, regex=True)
            return df

        def mode(self, columns):
            raise NotImplementedError("Not implemented error. See https://github.com/rapidsai/cudf/issues/3677")

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
            # Reference https://medium.com/rapids-ai/show-me-the-word-count-3146e1173801
            df = self

        def strip_html(self):
            df = self
            # soup = BeautifulSoup(self.text, "html.parser")
            # self.text = soup.get_text()
            return self

        def min_max_scaler(self, input_cols, output_cols=None):
            pass

        def standard_scaler(self, input_cols, output_cols=None):

            df = self
            scaler = StandardScaler()
            for input_col, output_col in zip(input_cols, output_cols):
                data = df[input_col]
                scaler.fit(data)
                df[output_col] = scaler.transform(data)

            return df

        def impute(self, input_cols, data_type="continuous", strategy="mean", output_cols=None):
            """

            :param input_cols:
            :param data_type:
            :param strategy:
            # - If "mean", then replace missing values using the mean along
            #   each column. Can only be used with numeric data.
            # - If "median", then replace missing values using the median along
            #   each column. Can only be used with numeric data.
            # - If "most_frequent", then replace missing using the most frequent
            #   value along each column. Can be used with strings or numeric data.
            # - If "constant", then replace missing values with fill_value. Can be
            #   used with strings or numeric data.
            :param output_cols:
            :return:
            """

            raise NotImplementedError("Not implemented yet")

        @staticmethod
        def max_abs_scaler(input_cols, output_cols=None):
            pass

        @staticmethod
        def find(input_cols, sub):
            """
            Find the position for a char or substring
            :param input_cols:
            :param sub:
            :return:
            """
            df = self

            def get_match_positions(_value, _separator):
                length = [[match.start(), match.end()] for match in re.finditer(_separator, _value)]
                return length if len(length) > 0 else None

            df["__match_positions__"] = df[input_cols].apply(get_match_positions, args=sub)
            return df

        @staticmethod
        def scatter(columns, buckets=10):
            pass

        @staticmethod
        def count_by_dtypes(columns, infer=False, str_funcs=None, int_funcs=None):
            df = self
            result = {}
            df_len = len(df)
            for col_name, na in df.cols.count_na(columns).items():
                result[col_name] = {"match": df_len - na, "missing": na, "mismatches": 0}
            return result

            # return np.count_nonzero(df.isnull().values.ravel())

        @staticmethod
        def correlation(input_cols, method="pearson", output="json"):
            pass

        @staticmethod
        def qcut(columns, num_buckets, handle_invalid="skip"):
            pass

        def string_to_index(self, input_cols=None, output_cols=None, columns=None):
            df = self.df
            le = preprocessing.LabelEncoder()
            return string_to_index(df, input_cols, output_cols, le)

        @staticmethod
        def index_to_string(input_cols=None, output_cols=None, columns=None):
            df = self.df
            le = preprocessing.LabelEncoder()
            return index_to_string(df, input_cols, output_cols, le)

    return Cols(self)


DataFrame.cols = property(cols)
