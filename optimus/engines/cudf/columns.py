import cudf
import cupy as cp
from cuml import preprocessing
from sklearn.preprocessing import StandardScaler

from optimus.engines.base.commons.functions import string_to_index, index_to_string, find, to_float_cudf, \
    to_string_cudf, to_integer_cudf
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.helpers.columns import parse_columns, get_output_cols
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.functions import set_function_parser, set_func
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import profiler_dtype_func, is_list_of_tuples


class Cols(DataFrameBaseColumns):
    def __init__(self, df):
        super(DataFrameBaseColumns, self).__init__(df)

    def _names(self):
        return list(self.parent.data.columns)

    def append(self, dfs):
        """

        :param dfs:
        :return:
        """

        df = self.parent
        df = cudf.concat([dfs.reset_index(drop=True), df.reset_index(drop=True)], axis=1)
        return df

    def _series_to_dict(self, series):
        return series.to_pandas().to_dict()

    def find(self, columns, sub, ignore_case=False):
        """
        Find the start and end position for a char or substring
        :param columns:
        :param ignore_case:
        :param sub:
        :return:
        """

        df = self.parent
        # TODO: This could be slow try to implement the find function in cudf
        df = df.to_pandas()

        df = find(df, columns, sub, ignore_case)

        return cudf.from_pandas(df)

    def to_string(self, input_cols="*", output_cols=None):
        df = self.parent
        return df.cols.apply(input_cols, to_string_cudf, output_cols=output_cols,
                             meta_action=Actions.TO_STRING.value,
                             mode="vectorized")

    def to_integer(self, input_cols="*", output_cols=None):

        df = self.parent
        return df.cols.apply(input_cols, to_integer_cudf, output_cols=output_cols,
                             meta_action=Actions.TO_INTEGER.value,
                             mode="pandas")

    def to_float(self, input_cols="*", output_cols=None):

        df = self.parent
        return df.cols.apply(input_cols, to_float_cudf, output_cols=output_cols, meta_action=Actions.TO_FLOAT.value,
                             mode="map")

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
        df = self.parent
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)
        for input_col, output_col in zip(input_cols, output_cols):
            df[output_col] = df[input_col].str.replace_multi(regex, replace_by, regex=True)
        return df

    # NLP
    def stem_words(self, input_col):
        raise NotImplementedError('Not implemented yet')

    def lemmatize_verbs(self, input_cols, output_cols=None):
        df = self.parent

        def func(value, args=None):
            return value + "aaa"

        df = df.cols.apply(input_cols, func, output_cols)
        return df

    def remove_stopwords(self):
        # Reference https://medium.com/rapids-ai/show-me-the-word-count-3146e1173801
        raise NotImplementedError('Not implemented yet')

    def strip_html(self):
        df = self.parent
        # soup = BeautifulSoup(self.text, "html.parser")
        # self.text = soup.get_text()
        raise NotImplementedError('Not implemented yet')

    def min_max_scaler(self, input_cols, output_cols=None):
        pass

    def standard_scaler(self, input_cols, output_cols=None):

        df = self.parent
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
        raise NotImplementedError('Not implemented yet')

    def is_match(self, columns, dtype, invert=False):
        """
        Find the rows that match a data type
        :param columns:
        :param dtype: data type to match
        :param invert: Invert the match
        :return:
        """
        df = self.parent
        columns = parse_columns(df, columns)

        f = profiler_dtype_func(dtype)
        if f is not None:
            for col_name in columns:
                df = df[col_name].to_pandas().apply(f)
                df = ~df if invert is True else df
            return df

    @staticmethod
    def scatter(columns, buckets=10):
        pass

    def hist(self, columns="*", buckets=20, compute=True):
        odf = self.parent
        columns = parse_columns(odf, columns)

        result = {}
        for col_name in columns:

            df_numeric = cp.array(to_float_cudf(odf.data[col_name]).to_gpu_array())

            if len(df_numeric) > 0:
                _count, _bins = cp.histogram(df_numeric, buckets)
                result[col_name] = [
                    {"lower": float(_bins[i]), "upper": float(_bins[i + 1]), "count": int(_count[i])}
                    for i in range(buckets)]

        return {"hist": result}

    def count_by_dtypes(self, columns, infer=False, str_funcs=None, int_funcs=None):
        df = self.parent
        result = {}
        df_len = len(df)
        for col_name, na in df.cols.count_na(columns).items():
            result[col_name] = {"match": df_len - na, "missing": na, "mismatches": 0}
        return result

        # return np.count_nonzero(df.isnull().values.ravel())

    @staticmethod
    def correlation(input_cols, method="pearson", output="json"):
        pass

    def qcut(self, columns, num_buckets, handle_invalid="skip"):
        pass

    def string_to_index(self, input_cols=None, output_cols=None, columns=None):
        df = self.parent
        le = preprocessing.LabelEncoder()
        return string_to_index(df, input_cols, output_cols, le)

    def index_to_string(self, input_cols=None, output_cols=None, columns=None):
        df = self.parent
        le = preprocessing.LabelEncoder()
        return index_to_string(df, input_cols, output_cols, le)

    def unnest(self, input_cols, separator=None, splits=-1, index=None, output_cols=None, drop=False,
               mode="string"):

        """
        Split an array or string in different columns
        :param input_cols: Columns to be un-nested
        :param output_cols: Resulted on or multiple columns after the unnest operation [(output_col_1_1,output_col_1_2),
        (output_col_2_1, output_col_2]
        :param separator: char or regex
        :param splits: Number of columns splits.
        :param index: Return a specific index per columns. [1,2]
        :param drop:
        :param mode:
        """
        odf = self.parent

        # if separator is not None:
        #     separator = re.escape(separator)

        input_cols = parse_columns(odf, input_cols)

        index = val_to_list(index)
        output_ordered_columns = odf.cols.names()

        for idx, input_col in enumerate(input_cols):

            if is_list_of_tuples(index):
                final_index = index[idx]
            else:
                final_index = index

            if mode == "string":

                df_new = df[input_col].astype(str).str.split(separator, expand=True, n=splits)
                final_splits = df_new.cols.count()
                if output_cols is None:
                    final_columns = [input_col + "_" + str(i) for i in range(final_splits)]
                elif is_list_of_tuples(output_cols):
                    final_columns = output_cols[idx]
                else:
                    final_columns = [output_cols + "_" + str(i) for i in range(final_splits)]

            # If columns split is shorter than the number of splits
            df_new.columns = final_columns[:len(df_new.columns)]
            if final_index:
                df_new = df_new.cols.select(final_index[idx])
            df = df.cols.append(df_new)

        if drop is True:
            df = df.drop(columns=input_cols)
            for input_col in input_cols:
                if input_col in output_ordered_columns:
                    output_ordered_columns.remove(input_col)

        df = df.meta.preserve(df, Actions.UNNEST.value, final_columns)

        return df.cols.move(df_new.cols.names(), "after", input_cols)

    def set(self, where=None, value=None, output_cols=None, default=None):
        """
        Set a column value using a number, string or a expression.
        :param where:
        :param value:
        :param output_cols:
        :param default:
        :return:
        """
        df = self.parent
        if output_cols is None:
            RaiseIt.value_error(output_cols, ["string"])

        columns, vfunc = set_function_parser(df, value, where, default)
        # if df.cols.dtypes(input_col) == "category":
        #     try:
        #         # Handle error if the category already exist
        #         df[input_vcol] = df[input_col].cat.add_categories(val_to_list(value))
        #     except ValueError:
        #         pass

        output_cols = one_list_to_val(output_cols)
        if columns:
            final_value = df[columns]
        else:
            final_value = df
        final_value = set_func(final_value, value=value, where=where, output_col=output_cols,
                               parser=vfunc, default=default)

        df.meta.preserve(df, Actions.SET.value, output_cols)
        kw_columns = {output_cols: final_value}
        return df.assign(**kw_columns)
