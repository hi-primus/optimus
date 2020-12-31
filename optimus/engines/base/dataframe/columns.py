from functools import reduce

import dask.dataframe as dd

from optimus.engines.base.columns import BaseColumns
from optimus.helpers.columns import parse_columns, get_output_cols


class DataFrameBaseColumns(BaseColumns):

    def __init__(self, df):
        super(DataFrameBaseColumns, self).__init__(df)

    def _map(self, df, input_col, output_col, func, args, kw_columns):
        kw_columns[output_col] = df[input_col].map(func, *args)
        return kw_columns

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
        arr = scaler.transform(df[input_cols])
        darr = dd.from_array(arr)
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

        df = self.root

        def _replace_regex(_value, _regex, _replace):
            return _value.replace(_regex, _replace, regex=True)

        return df.cols.apply(input_cols, func=_replace_regex, args=(regex, value,), output_cols=output_cols,
                             filter_col_by_dtypes=df.constants.STRING_TYPES + df.constants.NUMERIC_TYPES)

    def reverse(self, input_cols, output_cols=None):
        def _reverse(value):
            return str(value)[::-1]

        df = self.root
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

    def nest(self, input_cols, separator="", output_col=None, shape="string", drop=False):
        df = self.root

        dfd = df.data

        if output_col is None:
            RaiseIt.type_error(output_col, ["str"])

        input_cols = parse_columns(df, input_cols)

        output_ordered_columns = df.cols.names()

        # cudfd do nor support apply or agg join for this operation
        if shape == "vector" or shape == "array":
            raise NotImplementedError("Not implemented yet")
            # https://stackoverflow.com/questions/43898035/pandas-combine-column-values-into-a-list-in-a-new-column/43898233
            # t['combined'] = t.values.tolist()

            dfds = [dfd[input_col] for input_col in input_cols]
            dfd[output_col] = dfd[input_cols].values.tolist()
        elif shape == "string":
            dfds = [dfd[input_col].astype(str) for input_col in input_cols]
            dfd[output_col] = reduce((lambda x, y: x + separator + y), dfds)

        if output_col not in output_ordered_columns:
            col_index = output_ordered_columns.index(input_cols[-1]) + 1
            output_ordered_columns[col_index:col_index] = [output_col]

        if drop is True:
            for input_col in input_cols:
                if input_col in output_ordered_columns and input_col != output_col:
                    output_ordered_columns.remove(input_col)

        return self.root.new(dfd).cols.select(output_ordered_columns)
