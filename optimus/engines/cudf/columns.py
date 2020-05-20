import re
import unicodedata

import cupy as cp
import numpy as np
from cudf.core import DataFrame
from sklearn.preprocessing import StandardScaler

from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.helpers.check import equal_function
from optimus.helpers.columns import parse_columns, get_output_cols, check_column_numbers
from optimus.helpers.constants import ProfilerDataTypesQuality
from optimus.helpers.converter import cudf_to_pandas, pandas_to_cudf
from optimus.helpers.core import val_to_list
# DataFrame = pd.DataFrame
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_str, profiler_dtype_func, is_dict


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
        #     df[input_cols] = df[input_cols].astype("str")
        #
        #     return df
        #     # return df

        @staticmethod
        def astype(*args, **kwargs):
            pass

        @staticmethod
        def move(column, position, ref_col=None):
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
                    result = func(columns, args, df=df)

            return result

        @staticmethod
        def exec_agg(exprs):
            return exprs

        @staticmethod
        def reverse(input_cols, output_cols=None):
            pass

        @staticmethod
        def remove(columns, search=None, search_by="chars", output_cols=None):
            pass

        # @staticmethod
        # def remove_accents(input_cols, output_cols=None):
        #     """
        #     Remove accents in specific columns
        #     :param input_cols: '*', list of columns names or a single column name.
        #     :param output_cols:
        #     :return:
        #     """
        #
        #     print(unicodedata.normalize('NFKD', "cell_str"))
        #
        #     def _remove_accents(value, attr):
        #         cell_str = str(value)
        #
        #         # first, normalize strings:
        #         nfkd_str = unicodedata.normalize('NFKD', cell_str)
        #
        #         # Keep chars that has no other char combined (i.e. accents chars)
        #         with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
        #         return with_out_accents
        #
        #     df = Cols.apply(input_cols, _remove_accents, "string", output_cols=output_cols,
        #                     meta=Actions.REMOVE_ACCENTS.value)
        #     return df

        @staticmethod
        def date_transform(input_cols, current_format=None, output_format=None, output_cols=None):
            pass

        @staticmethod
        def years_between(input_cols, date_format=None, output_cols=None):
            pass

        def mode(self, columns):
            df = self.df
            columns = parse_columns(df, columns)
            result = {}
            for col_name in columns:
                result[col_name] = df[col_name].value_counts().index[0]

            return result

        # https://github.com/rapidsai/cudf/issues/3177
        def replace(self, input_cols, search=None, replace_by=None, search_by="chars", output_cols=None):
            """
            Replace a value, list of values by a specified string
            :param input_cols: '*', list of columns names or a single column name.
            :param output_cols:
            :param search: Values to look at to be replaced
            :param replace_by: New value to replace the old one
            :param search_by: Can be "full","words","chars" or "numeric".
            :return: Dask DataFrame
            """

            df = self.df
            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)

            search = val_to_list(search)
            if search_by == "chars":
                _regex = search
                # _regex = re.compile("|".join(map(re.escape, search)))
            elif search_by == "words":
                _regex = (r'\b%s\b' % r'\b|\b'.join(map(re.escape, search)))
            else:
                _regex = search

            # df = df.cols.cast(input_cols, "str")

            if not is_str(replace_by):
                RaiseIt.type_error(replace_by, ["str"])

            for input_col, output_col in zip(input_cols, output_cols):
                if search_by == "chars" or search_by == "words":
                    # This is only implemented in cudf
                    df[output_col] = df[input_col].str.replace_multi(search, replace_by, regex=False)
                    # df[input_col] = df[input_col].str.replace(search, replace_by)
                elif search_by == "full":
                    df[output_col] = df[input_col].replace(search, replace_by)

            return df

        def replace_regex(self, input_cols, regex=None, replace_by=None, output_cols=None):
            df = self.df
            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)
            for input_col, output_col in zip(input_cols, output_cols):
                df[output_col] = df[input_col].str.replace_multi(regex, replace_by, regex=True)
            return df

        @staticmethod
        def impute(input_cols, data_type="continuous", strategy="mean", output_cols=None):
            pass

        @staticmethod
        def is_na(input_cols, output_cols=None):
            pass

        @staticmethod
        def count_na(columns):
            df = self
            columns = parse_columns(df, columns)
            result = {}

            def _count_na(_df, _series):
                return cp.count_nonzero(_df[_series].isnull().values.ravel())

            for col_name in columns:
                result[col_name] = _count_na(df, col_name)
                # np.count_nonzero(df[col_name].isnull().values.ravel())
            return result

        @staticmethod
        def count_uniques(columns, estimate=True):
            pass

        @staticmethod
        def extract(input_cols, output_cols, regex):
            # Cudf no support regex to we convert this to pandas, operate and the convert back to cudf
            df = self
            from optimus.engines.base.dataframe.commons import extract
            df = cudf_to_pandas(df)
            df = extract(df, input_cols, output_cols, regex)
            df = pandas_to_cudf(df)
            return df

        @staticmethod
        def unique(columns):
            pass

        @staticmethod
        def nunique_approx(columns):
            df = self
            return df.cols.nunique(columns)

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

        def remove_numbers(self):
            df = self
            self.text = re.sub('[-+]?[0-9]+', '', self.text)
            return self

        def strip_html(self):
            df = self
            # soup = BeautifulSoup(self.text, "html.parser")
            # self.text = soup.get_text()
            return self

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

            df = self
            scaler = StandardScaler()
            for input_col, output_col in zip(input_cols, output_cols):
                data = df[input_col]
                scaler.fit(data)
                df[output_col] = scaler.transform(data)

            return df

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
                result[col_name] = {"match": df_len - na, "missing": na, "mismatches": 0}
            return result

            # return np.count_nonzero(df.isnull().values.ravel())

        def remove_accents(self, input_cols, output_cols=None):

            # pandas_to_cudf()
            # cudf_to_pandas
            def _remove_accents(value):
                cell_str = str(value)

                # first, normalize strings:
                nfkd_str = unicodedata.normalize('NFKD', cell_str)

                # Keep chars that has no other char combined (i.e. accents chars)
                with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
                return with_out_accents

            df = self.df
            return df.cols.apply(input_cols, _remove_accents, func_return_type=str,
                                 filter_col_by_dtypes=df.constants.STRING_TYPES,
                                 output_cols=output_cols)

        def remove_special_chars(self, input_cols, output_cols=None):
            df = self.df
            input_cols = parse_columns(df, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)

            df = df.cols.cast(input_cols, "str")
            for input_col, output_col in zip(input_cols, output_cols):
                df[output_col].str.replace_multi(["[^A-Za-z0-9]+"], "", regex=True)
            return df

        def min(self, columns):

            df = self.df
            columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
            r = {}
            for col_name in columns:
                r[col_name] = df[col_name].min()

            return r
            # return {column: _min for column, _min in zip(columns, min_values)}

        def max(self, columns):

            df = self.df
            columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
            r = {}
            for col_name in columns:
                r[col_name] = df[col_name].max()

            return r

        def count_uniques(self, columns, estimate=True):
            df = self.df
            columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
            r = {}
            for col_name in columns:
                r[col_name] = df[col_name].nunique()

            return r

        def hist(self, columns, buckets=10, compute=True):
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
        def clip(columns, lower_bound, upper_bound):
            pass

        @staticmethod
        def values_to_cols(input_cols):
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
            for col_name in columns:
                result[col_name] = df[col_name].nunique()

            return result

        def count_mismatch(self, columns_mismatch: dict = None, infer=True):
            # print("infer", columns_mismatch)
            df = self.df
            if not is_dict(columns_mismatch):
                columns_mismatch = parse_columns(df, columns_mismatch)

            def count_dtypes(_df, _col_name, _func_dtype):

                def _func(value):
                    # null values

                    # match data type
                    if _func_dtype(value):
                        # ProfilerDataTypesQuality.MATCH.value
                        return 2

                    elif pd.isnull(value):
                        # ProfilerDataTypesQuality.MISSING.value
                        return 1

                    # mismatch
                    else:
                        # ProfilerDataTypesQuality.MISMATCH.value
                        return 0

                return _df[_col_name].map(_func).value_counts()

            df_len = len(df)

            def no_infer_func(_df, _col_name, _nulls_count):

                return pd.Series({ProfilerDataTypesQuality.MATCH.value: df_len - _nulls_count[_col_name],
                                  ProfilerDataTypesQuality.MISSING.value: _nulls_count[_col_name]}, name=_col_name)

            if infer is True:
                delayed_parts = [count_dtypes(df, col_name, profiler_dtype_func(dtype, True)) for col_name, dtype in
                                 columns_mismatch.items()]

            else:
                nulls_count = df.isna().sum().compute().to_dict()
                delayed_parts = [no_infer_func(df, col_name, nulls_count) for col_name in columns_mismatch]

            def merge(value):
                return [{v.name: v.to_dict()} for v in value]

            b = merge(delayed_parts)

            c = b
            # Flat dict
            d = {x: y for _c in c for x, y in _c.items()}

            def none_to_zero(value):
                return value if value is not None else 0

            result = {}

            for col_name, values in d.items():
                result[col_name] = {"mismatch": none_to_zero(values.get(ProfilerDataTypesQuality.MISMATCH.value)),
                                    "missing": none_to_zero(values.get(ProfilerDataTypesQuality.MISSING.value)),
                                    "match": none_to_zero(values.get(ProfilerDataTypesQuality.MATCH.value))
                                    }
                if infer is True:
                    result[col_name].update({"profiler_dtype": columns_mismatch[col_name]})

            return result

        def count(self):
            df = self.df
            return len(df)

        @staticmethod
        def frequency(columns, n=10, percentage=False, count_uniques=False, total_rows=None, *args, **kwargs):
            # https://stackoverflow.com/questions/10741346/numpy-most-efficient-frequency-counts-for-unique-values-in-an-array
            df = self

            columns = parse_columns(df, columns)

            result = {}
            for col_name in columns:
                # Value counts
                r = df[col_name].value_counts().nlargest(n)

                i = list(r.index)
                j = r.tolist()

                result[col_name] = ({"values": i, "count": j})
            return result

    return Cols(self)


DataFrame.cols = property(cols)