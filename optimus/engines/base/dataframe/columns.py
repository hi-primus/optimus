import re
from functools import reduce

import dask.dataframe as dd

from optimus.engines.base.columns import BaseColumns
from optimus.helpers.check import is_cudf_dataframe
from optimus.helpers.columns import parse_columns, get_output_cols
from optimus.helpers.core import val_to_list
from optimus.infer import is_str


class DataFrameBaseColumns(BaseColumns):

    def __init__(self, df):
        super(DataFrameBaseColumns, self).__init__(df)

    @staticmethod
    def exec_agg(exprs, compute=None):
        """
        Exectute and aggregation
        Expression in Non dask dataframe can not handle compute. See exec_agg dask implementation
        :param exprs:
        :param compute:
        :return:
        """
        return exprs

    def qcut(self, columns, num_buckets, handle_invalid="skip"):
        pass

    @staticmethod
    def correlation(input_cols, method="pearson", output="json"):
        pass

    @staticmethod
    def scatter(columns, buckets=10):
        pass

    @staticmethod
    def standard_scaler(self, input_cols, output_cols=None):
        pass

    @staticmethod
    def max_abs_scaler(input_cols, output_cols=None):
        pass

    def min_max_scaler(self, input_cols, output_cols=None):
        # https://github.com/dask/dask/issues/2690

        df = self.df

        scaler = MinMaxScaler()

        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        # _df = df[input_cols]
        scaler.fit(df[input_cols])
        # print(type(scaler.transform(_df)))
        arr = scaler.transform(df[input_cols])
        darr = dd.from_array(arr)
        # print(type(darr))
        darr.name = 'z'
        df = df.merge(darr)

        return df

    def replace_regex(self, input_cols, regex=None, value="", output_cols=None):
        """
        Use a Regex to replace values
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param regex: values to look at to be replaced
        :param value: new value to replace the old one
        :return:
        """

        df = self.df

        def _replace_regex(_value, _regex, _replace):
            return _value.replace(_regex, _replace, regex=True)

        return df.cols.apply(input_cols, func=_replace_regex, args=(regex, value,), output_cols=output_cols,
                             filter_col_by_dtypes=df.constants.STRING_TYPES + df.constants.NUMERIC_TYPES)

    def find(self, columns, sub, ignore_case=False):
        """
        Find the start and end position for a char or substring
        :param columns:
        :param ignore_case:
        :param sub:
        :return:
        """
        df = self.df
        # TODO Find a way to implement this in cudf. This could be slow when big operations.
        if is_cudf_dataframe(df):
            _df = df.to_pandas()
        else:
            _df = df

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
            _df[col_name + "__match_positions__"] = _df[col_name].astype("object").apply(get_match_positions,
                                                                                       args=(sub,))

        if is_cudf_dataframe(df):
            import cudf
            df = cudf.from_pandas(_df)

        return df

    def reverse(self, input_cols, output_cols=None):
        def _reverse(value):
            return str(value)[::-1]

        df = self.df
        return df.cols.apply(input_cols, _reverse, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, set_index=True)

    @staticmethod
    def astype(*args, **kwargs):
        pass

    @staticmethod
    def apply_by_dtypes(columns, func, func_return_type, args=None, func_type=None, data_type=None):
        pass

    @staticmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        pass

    def nest(self, input_cols, shape="string", separator="", output_col=None):
        df = self.df

        if output_col is None:
            output_col = "_".join(input_cols)

        input_cols = parse_columns(df, input_cols)

        # cudf do nor support apply or agg join for this operation
        if shape == "vector" or shape == "array":
            raise NotImplementedError("Not implemented yet")
            # https://stackoverflow.com/questions/43898035/pandas-combine-column-values-into-a-list-in-a-new-column/43898233
            # t['combined'] = t.values.tolist()

            dfs = [df[input_col] for input_col in input_cols]
            df[output_col] = df[input_cols].values.tolist()
        elif shape == "string":
            dfs = [df[input_col].astype(str) for input_col in input_cols]
            df[output_col] = reduce((lambda x, y: x + separator + y), dfs)
        return df
