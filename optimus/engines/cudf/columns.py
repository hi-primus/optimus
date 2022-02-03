import builtins
from sklearn.preprocessing import StandardScaler

from optimus.engines.base.commons.functions import string_to_index, index_to_string, find
from optimus.engines.base.cudf.columns import CUDFBaseColumns
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.helpers.columns import parse_columns, get_output_cols
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_dict, is_list, is_tuple


class Cols(CUDFBaseColumns, DataFrameBaseColumns):

    def __init__(self, df):
        super().__init__(df)

    def _series_to_pandas(self, series):
        return series.to_pandas()

    def find(self, cols, sub, ignore_case=False):
        """
        Find the start and end position for a char or substring
        :param cols:
        :param ignore_case:
        :param sub:
        :return:
        """
        import cudf

        df = self.root
        # TODO: This could be slow try to implement the find function in cudf
        df = df.to_pandas()

        df = find(df, cols, sub, ignore_case)

        return cudf.from_pandas(df)

    def to_timestamp(self, cols="*", date_format=None, output_cols=None):
        raise NotImplementedError('Not implemented yet')

    def reverse(self, cols, output_cols=None):
        raise NotImplementedError('Not implemented yet')

    # https://github.com/rapidsai/cudf/issues/3177
    # def replace(self, cols, search=None, replace_by=None, search_by="chars", output_cols=None):
    #     """
    #     Replace a value, list of values by a specified string
    #     :param cols: '*', list of columns names or a single column name.
    #     :param output_cols:
    #     :param search: Values to look at to be replaced
    #     :param replace_by: New value to replace the old one
    #     :param search_by: Can be "full","words","chars" or "numeric".
    #     :return: Dask DataFrame
    #     """
    #
    #     df = self.df
    #     columns = prepare_columns(df, cols, output_cols)
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

    def replace_regex(self, cols, regex=None, replace_by=None, output_cols=None):
        df = self.root
        cols = parse_columns(df, cols)
        output_cols = get_output_cols(cols, output_cols)
        for input_col, output_col in zip(cols, output_cols):
            df[output_col] = df[input_col].str.replace_multi(regex, replace_by, regex=True)
        return df

    # NLP
    def stem_words(self, input_col):
        raise NotImplementedError('Not implemented yet')

    def strip_html(self):
        # df = self.root
        # soup = BeautifulSoup(self.text, "html.parser")
        # self.text = soup.get_text()
        raise NotImplementedError('Not implemented yet')

    def min_max_scaler(self, cols="*", output_cols=None):
        raise NotImplementedError('Not implemented yet')

    def standard_scaler(self, cols, output_cols=None):

        df = self.root
        scaler = StandardScaler()
        for input_col, output_col in zip(cols, output_cols):
            data = df[input_col]
            scaler.fit(data)
            df[output_col] = scaler.transform(data)

        return df

    @staticmethod
    def impute(cols, data_type="continuous", strategy="mean", fill_value=None, output_cols=None):
        raise NotImplementedError("Not implemented yet")

    def max_abs_scaler(self, cols="*", output_cols=None):
        raise NotImplementedError('Not implemented yet')

    def is_match(self, cols="*", data_type=None, invert=False):
        """
        Find the rows that match a data type
        :param cols:
        :param data_type: data type to match
        :param invert: Invert the match
        :return:
        """
        df = self.root
        cols = parse_columns(df, cols)

        f = inferred_data_type_func(data_type)
        if f is not None:
            for col_name in cols:
                df = df[col_name].to_pandas().apply(f)
                df = ~df if invert is True else df
            return df

    def hist(self, cols="*", buckets=20, range=None, compute=True):
        import cupy as cp
        df = self.root
        cols = parse_columns(df, cols)

        if range is not None and (is_tuple(range) or is_list(range)):
            _min = {col: range[0] for col in cols}
            _max = {col: range[1] for col in cols}
        elif range is not None and is_dict(range):
            _min = {col: range[col][0] for col in cols}
            _max = {col: range[col][1] for col in cols}
        else:
            _min = None
            _max = None

        result = {}
        for col_name in cols:
            # Seems to be some kind of bug of some types of data sets that converts all values to nan,
            # we drop na before  converting to array
            df_numeric = cp.array(self.F.to_float(df.data[col_name]).dropna().to_cupy())
            if len(df_numeric) > 0:
                _range = (_min[col_name], _max[col_name]) if _min is not None and _max is not None else None
                _count, _bins = cp.histogram(df_numeric, buckets, range=_range)
                result[col_name] = [
                    {"lower": float(_bins[i]), "upper": float(_bins[i + 1]), "count": int(_count[i])}
                    for i in builtins.range(buckets)]

        return {"hist": result}

    def qcut(self, cols="*", quantiles=None, handle_invalid="skip"):
        raise NotImplementedError('Not implemented yet')

    def string_to_index(self, cols=None, output_cols=None):
        from cuml import preprocessing
        df = self.root
        df.le = df.le or preprocessing.LabelEncoder()
        return string_to_index(df, cols, output_cols, df.le)

    def index_to_string(self, cols=None, output_cols=None):
        from cuml import preprocessing
        df = self.root
        df.le = df.le or preprocessing.LabelEncoder()
        return index_to_string(df, cols, output_cols, df.le)

    def _unnest(self, dfd, input_col, final_columns, separator, splits, mode, output_cols):
        if mode == "string":
            dfd_new = dfd[input_col].astype(str).str.split(separator, expand=True, n=splits)

        else:
            RaiseIt.value_error(mode, ["string"])

        return dfd_new
