import pandas as pd
from sklearn import preprocessing

from optimus.engines.base.commons.functions import impute, string_to_index, index_to_string, find
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns

DataFrame = pd.DataFrame


class Cols(DataFrameBaseColumns):
    def __init__(self, df):
        super(DataFrameBaseColumns, self).__init__(df)

    def _names(self):
        return list(self.root.data.columns)

    def append(self, dfs):
        """

        :param dfs:
        :return:
        """

        df = self.root
        dfd = pd.concat([*[_df.data.reset_index(drop=True) for _df in dfs], df.data.reset_index(drop=True)], axis=1)
        return self.root.new(dfd)

    def find(self, columns, sub, ignore_case=False):
        """
        Find the start and end position for a char or substring
        :param columns:
        :param ignore_case:
        :param sub:
        :return:
        """
        df = self.root
        return find(df, columns, sub, ignore_case)

    @staticmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        pass

    def impute(self, input_cols, data_type="continuous", strategy="mean", fill_value=None, output_cols=None):
        df = self.root
        return impute(df, input_cols, data_type=data_type, strategy=strategy, fill_value=fill_value, output_cols=None)

    @staticmethod
    def astype(*args, **kwargs):
        pass

    # NLP
    @staticmethod
    def stem_words(input_col):
        df = self

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
        df = self.root
        result = {}
        df_len = len(df.data)
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

    def string_to_index(self, input_cols="*", output_cols=None, columns=None):
        df = self.root
        le = preprocessing.LabelEncoder()
        df = string_to_index(df, input_cols, output_cols, le)

        return df

    def index_to_string(self, input_cols=None, output_cols=None, columns=None):
        df = self.root
        le = preprocessing.LabelEncoder()
        df = index_to_string(df, input_cols, output_cols, le)

        return df
