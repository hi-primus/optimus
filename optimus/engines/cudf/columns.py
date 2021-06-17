from cuml import preprocessing
from sklearn.preprocessing import StandardScaler

from optimus.engines.base.commons.functions import string_to_index, index_to_string, find, to_float_cudf, \
    to_string_cudf, to_integer_cudf
from optimus.engines.base.cudf.columns import CUDFBaseColumns
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.engines.base.columns import BaseColumns
from optimus.engines.base.meta import Meta
from optimus.helpers.columns import parse_columns, get_output_cols
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_tuples


class Cols(CUDFBaseColumns, DataFrameBaseColumns, BaseColumns):

    def __init__(self, df):
        super().__init__(df)

    @property
    def _pd(self):
        import cudf
        return cudf

    def _series_to_pandas(self, series):
        return series.to_pandas()

    @staticmethod
    def exec_agg(exprs, compute):
        """
        Execute and aggregation
        :param exprs:
        :return:
        """
        # print("exprs",type(exprs))
        try:
            return exprs[0].to_pandas().to_dict()
        except TypeError:
            return exprs
        except AttributeError:
            return exprs
        except IndexError:
            return exprs

    def find(self, columns, sub, ignore_case=False):
        """
        Find the start and end position for a char or substring
        :param columns:
        :param ignore_case:
        :param sub:
        :return:
        """
        import cudf

        df = self.root
        # TODO: This could be slow try to implement the find function in cudf
        df = df.to_pandas()

        df = find(df, columns, sub, ignore_case)

        return cudf.from_pandas(df)

    def to_string(self, input_cols="*", output_cols=None):
        df = self.root
        return df.cols.apply(input_cols, to_string_cudf, output_cols=output_cols,
                             meta_action=Actions.TO_STRING.value,
                             mode="vectorized")

    def to_integer(self, input_cols="*", output_cols=None):

        df = self.root
        return df.cols.apply(input_cols, to_integer_cudf, output_cols=output_cols,
                             meta_action=Actions.TO_INTEGER.value,
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
        df = self.root
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)
        for input_col, output_col in zip(input_cols, output_cols):
            df[output_col] = df[input_col].str.replace_multi(regex, replace_by, regex=True)
        return df

    # NLP
    def stem_words(self, input_col):
        raise NotImplementedError('Not implemented yet')


    def strip_html(self):
        df = self.root
        # soup = BeautifulSoup(self.text, "html.parser")
        # self.text = soup.get_text()
        raise NotImplementedError('Not implemented yet')

    def min_max_scaler(self, input_cols, output_cols=None):
        pass

    def standard_scaler(self, input_cols, output_cols=None):

        df = self.root
        scaler = StandardScaler()
        for input_col, output_col in zip(input_cols, output_cols):
            data = df[input_col]
            scaler.fit(data)
            df[output_col] = scaler.transform(data)

        return df

    def impute(self, input_cols, data_type="continuous", strategy="mean", fill_value=None, output_cols=None):
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
        df = self.root
        columns = parse_columns(df, columns)

        f = profiler_dtype_func(dtype)
        if f is not None:
            for col_name in columns:
                df = df[col_name].to_pandas().apply(f)
                df = ~df if invert is True else df
            return df

    @staticmethod
    def heatmap(columns, buckets=10):
        pass

    def hist(self, columns="*", buckets=20, compute=True):
        import cupy as cp
        df = self.root
        columns = parse_columns(df, columns)

        result = {}
        for col_name in columns:
            # Seems to be some kind of bug of some types of data sets that converts all values to nan,
            # we drop na before  converting to array
            df_numeric = cp.array(to_float_cudf(df.data[col_name]).dropna().to_gpu_array())
            if len(df_numeric) > 0:
                _count, _bins = cp.histogram(df_numeric, buckets)
                result[col_name] = [
                    {"lower": float(_bins[i]), "upper": float(_bins[i + 1]), "count": int(_count[i])}
                    for i in range(buckets)]

        return {"hist": result}

    def count_by_dtypes(self, columns, infer=False, str_funcs=None, int_funcs=None):
        df = self.root
        result = {}
        df_len = len(df)
        for col_name, na in df.cols.count_na(columns).items():
            result[col_name] = {"match": df_len - na, "missing": na, "mismatches": 0}
        return result

        # return np.count_nonzero(df.isnull().values.ravel())

    def qcut(self, columns, num_buckets, handle_invalid="skip"):
        pass

    def string_to_index(self, input_cols=None, output_cols=None, columns=None):
        df = self.root
        le = preprocessing.LabelEncoder()
        return string_to_index(df, input_cols, output_cols, le)

    def index_to_string(self, input_cols=None, output_cols=None, columns=None):
        df = self.root
        le = preprocessing.LabelEncoder()
        return index_to_string(df, input_cols, output_cols, le)

    def _unnest(self, dfd, input_col, final_columns, separator, splits, mode, output_cols):
        if mode == "string":
            dfd_new = dfd[input_col].astype(str).str.split(separator, expand=True, n=splits)

        else:
            RaiseIt.value_error(mode, ["string"])

        return dfd_new
