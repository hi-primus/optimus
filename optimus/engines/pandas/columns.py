import pandas as pd
from sklearn import preprocessing

from optimus.engines.base.commons.functions import to_integer, to_float, impute, string_to_index, index_to_string, find
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.helpers.constants import Actions

DataFrame = pd.DataFrame


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

    def find(self, columns, sub, ignore_case=False):
        """
        Find the start and end position for a char or substring
        :param columns:
        :param ignore_case:
        :param sub:
        :return:
        """
        df = self.df
        return find(df, columns, sub, ignore_case)

    @staticmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        pass

    # def to_string(self, input_cols, output_cols=None):
    #     df = self.df
    #
    #     return df.cols.apply(input_cols, str, output_cols=output_cols, meta_action=Actions.TO_STRING.value,
    #                          mode="vectorized")

    # def to_integer(self, input_cols, output_cols=None):
    #     df = self.df
    #
    #     return df.cols.apply(input_cols, to_integer, output_cols=output_cols, meta_action=Actions.TO_INTEGER.value,
    #                          mode="map")
    #
    # def to_float(self, input_cols="*", output_cols=None):
    #     df = self.df
    #
    #     return df.cols.apply(input_cols, to_float, output_cols=output_cols, meta_action=Actions.TO_FLOAT.value,
    #                          mode="map")

    def impute(self, input_cols, data_type="continuous", strategy="mean", output_cols=None):
        df = self.df
        return impute(df, input_cols, data_type="continuous", strategy="mean", output_cols=None)

    @staticmethod
    def astype(*args, **kwargs):
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
    def scatter(columns, buckets=10):
        pass

    def count_by_dtypes(self, columns, dtype):
        df = self.parent.data
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
