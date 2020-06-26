import re

import cudf
import cupy as cp
from cudf.core import DataFrame
from sklearn.preprocessing import StandardScaler

from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.helpers.columns import parse_columns, get_output_cols
# DataFrame = pd.DataFrame
from optimus.infer import is_dict, regex_credit_card, regex_zip_code, regex_url, \
    regex_gender, regex_boolean, regex_ip, regex_email, regex_decimal, regex_int


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

        @staticmethod
        def to_timestamp(input_cols, date_format=None, output_cols=None):
            pass

        @staticmethod
        def set(output_col, value=None):
            pass

        @staticmethod
        def exec_agg(exprs):
            return exprs

        def reverse(self, input_cols, output_cols=None):
            raise NotImplementedError('Not implemented yet')

        def weekofyear(self, input_cols, output_cols=None):
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
            df = self

        def strip_html(self):
            df = self
            # soup = BeautifulSoup(self.text, "html.parser")
            # self.text = soup.get_text()
            return self

        @staticmethod
        def select_by_dtypes(data_type):
            pass

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

        def remove_accents(self, input_cols, output_cols=None):
            raise NotImplementedError('Not implemented yet')

        def hist(self, columns="*", buckets=10, compute=True):
            df = self.df
            columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)

            def hist_series(_series, _buckets):
                # .to_gpu_array filter nan
                i, j = cp.histogram(cp.array(_series.to_gpu_array()), _buckets)

                i = list(i)
                j = list(j)
                _hist = [{"lower": float(j[index]), "upper": float(j[index + 1]), "count": int(i[index])} for index in
                         range(len(i))]

                return {_series.name: {"hist": _hist}}

            columns = parse_columns(df, columns)
            r = [hist_series(df[col_name], buckets) for col_name in columns]

            # Flat list of dict
            r = {x: y for i in r for x, y in i.items()}

            return r

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
            pass
            # from cudf import DataFrame, Series
            #
            # data = DataFrame({'category': ['a', 'b', 'c', 'd']})
            #
            # # There are two functionally equivalent ways to do this
            # le = LabelEncoder()
            # le.fit(data.category)  # le = le.fit(data.category) also works
            # encoded = le.transform(data.category)
            #
            # print(encoded)
            #
            # # This method is preferred
            # le = LabelEncoder()
            # encoded = le.fit_transform(data.category)
            #
            # print(encoded)
            #
            # # We can assign this to a new column
            # data = data.assign(encoded=encoded)
            # print(data.head())
            #
            # # We can also encode more data
            # test_data = Series(['c', 'a'])
            # encoded = le.transform(test_data)
            # print(encoded)
            #
            # # After train, ordinal label can be inverse_transform() back to
            # # string labels
            # ord_label = cudf.Series([0, 0, 1, 2, 1])
            # ord_label = dask_cudf.from_cudf(data, npartitions=2)
            # str_label = le.inverse_transform(ord_label)
            # print(str_label)
            #

        @staticmethod
        def index_to_string(input_cols=None, output_cols=None, columns=None):
            pass

        @staticmethod
        def bucketizer(input_cols, splits, output_cols=None):
            pass

        def count_mismatch(self, columns_mismatch: dict = None, **kwargs):
            df = self.df
            if not is_dict(columns_mismatch):
                columns_mismatch = parse_columns(df, columns_mismatch)

            result = {}
            nulls = df.isnull().sum().to_pandas().to_dict()
            total_rows = len(df)

            func = {"int": regex_int,  # Test this cudf.Series(cudf.core.column.string.cpp_is_integer(a["A"]._column))
                    "decimal": regex_decimal,
                    "email": regex_email,
                    "ip": regex_ip,
                    "url": regex_url,
                    "gender": regex_gender,
                    "boolean": regex_boolean,
                    "zip_code": regex_zip_code,
                    "credit_card_number": regex_credit_card,
                    "date": r"",
                    "object": r"",
                    "array": r""
                    }

            for col_name, dtype in columns_mismatch.items():
                result[col_name] = {"match": 0, "missing": 0, "mismatch": 0}
                result[col_name]["missing"] = nulls.get(col_name)
                matches_count = {True: 0, False: 0}

                if dtype == "string":
                    matches_count[True] = total_rows - nulls[col_name]
                    matches_count[False] = nulls[col_name]

                else:
                    matches_count = df[col_name].str.match(func[dtype]).value_counts().to_pandas().to_dict()

                match = matches_count.get(True)
                mismatch = matches_count.get(False)
                # print("mismatch", mismatch, match, matches_count)

                result[col_name]["match"] = 0 if match is None else match
                result[col_name]["mismatch"] = 0 if mismatch is None else mismatch

            for col_name in columns_mismatch.keys():
                result[col_name].update({"profiler_dtype": columns_mismatch[col_name]})
            return result

        @staticmethod
        def frequency(columns, n=10, percentage=False, count_uniques=False, total_rows=None, *args, **kwargs):
            df = self

            columns = parse_columns(df, columns)

            result = {}
            for col_name in columns:
                values_and_count = df[col_name].value_counts()

                value_count = [{"values": value, "count": count} for value, count in
                               values_and_count.nlargest(n).to_pandas().items()]
                if count_uniques is True:
                    result[col_name] = {"frequency": value_count, "count_uniques": len(values_and_count)}
                else:
                    result[col_name] = {"frequency": value_count}
            return result

    return Cols(self)


DataFrame.cols = property(cols)
