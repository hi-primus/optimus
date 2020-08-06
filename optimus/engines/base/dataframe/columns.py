from functools import reduce

import dask.dataframe as dd

from optimus.engines.base.columns import BaseColumns
# from optimus.engines.base.functions import to_numeric
from optimus.helpers.columns import parse_columns, get_output_cols
from optimus.helpers.core import one_list_to_val
from optimus.helpers.functions import set_function_parser, set_func


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

    def replace_regex(self, input_cols, regex=None, value=None, output_cols=None):
        """
        Use a Regex to replace values
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param regex: values to look at to be replaced
        :param value: new value to replace the old one
        :return:
        """

        df = self.df

        def _replace_regex(value, regex, replace):
            return value.replace(regex, replace)

        return df.cols.apply(input_cols, func=_replace_regex, args=[regex, value], output_cols=output_cols,
                             filter_col_by_dtypes=df.constants.STRING_TYPES + df.constants.NUMERIC_TYPES)

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

    def set(self, where=None, value=None, output_cols=None, default=None):

        df = self.df

        columns, vfunc = set_function_parser(df, value, where, default)
        # if df.cols.dtypes(input_col) == "category":
        #     try:
        #         # Handle error if the category already exist
        #         df[input_col] = df[input_col].cat.add_categories(val_to_list(value))
        #     except ValueError:
        #         pass

        output_cols = one_list_to_val(output_cols)

        if columns:
            final_value = set_func(df[columns], value=value, where=where, output_col=output_cols, parser=vfunc,
                                   default=default)
        else:
            final_value = set_func(df, value=value, where=where, output_col=output_cols, parser=vfunc,
                                   default=default)

        kw_columns = {output_cols: final_value}
        return df.assign(**kw_columns)

    # # TODO: Check if we must use * to select all the columns

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

    # def is_numeric(self, col_name):
    #     """
    #     Check if a column is numeric
    #     :param col_name:
    #     :return:
    #     """
    #     df = self.df
    #     # TODO: Check if this is the best way to check the data type
    #     if np.dtype(df[col_name]).type in [np.int64, np.int32, np.float64]:
    #         result = True
    #     else:
    #         result = False
    #     return result
