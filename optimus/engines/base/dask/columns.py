import dask
import dask.dataframe as dd
import pandas as pd
from dask import delayed

from optimus.engines.base.meta import Meta
from optimus.helpers.columns import parse_columns, name_col
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list
from optimus.profiler.functions import fill_missing_var_types

from optimus.engines.base.distributed.columns import DistributedBaseColumns


class DaskBaseColumns(DistributedBaseColumns):

    @staticmethod
    def exec_agg(exprs, compute):
        """
        Execute and aggregation
        :param exprs:
        :param compute:
        :return:
        """

        if compute:
            result = dask.compute(exprs)
            while isinstance(result, (list, tuple)) and len(result) == 1:
                result = result[0]
            if getattr(result, "to_dict", None):
                result = result.to_dict()
        else:
            result = delayed(exprs)

        return result

    def append(self, dfs):
        """

        :param dfs:
        :return:
        """

        dfs = val_to_list(dfs)

        df = self.root
        dfd = dd.concat([df.data.reset_index(drop=True), *[_df.data.reset_index(drop=True) for _df in dfs]], axis=1)
        meta = Meta.action(df.meta, Actions.APPEND.value, df.cols.names())
        return self.root.new(dfd, meta=meta)
        # return dfd

    def qcut(self, cols="*", quantiles=4, handle_invalid="skip"):

        df = self.root.data
        cols = parse_columns(df, cols)
        df[cols] = df[cols].map_partitions(pd.qcut, quantiles)
        return df

    def heatmap(self, cols="*", buckets=10):
        raise NotImplementedError('Not implemented yet')

    # Date operations
    def to_timestamp(self, cols="*", date_format=None, output_cols=None):
        raise NotImplementedError('Not implemented yet')

    def replace_regex(self, cols="*", regex=None, value="", output_cols=None):
        """
        Use a Regex to replace values
        :param cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param regex: values to look at to be replaced
        :param value: new value to replace the old one
        :return:
        """

        def _replace_regex(_value, _regex, _replace):
            return _value.replace(_regex, _replace, regex=True)

        return self.apply(cols, func=_replace_regex, args=(regex, value,), output_cols=output_cols,
                          filter_col_by_dtypes=self.root.constants.STRING_TYPES + self.root.constants.NUMERIC_TYPES)

    def astype(self, cols="*", output_cols=None, *args, **kwargs):
        raise NotImplementedError('Not implemented yet')

    # TODO: Check if we must use * to select all the columns

    def count_by_data_types(self, cols="*", infer=False, str_funcs=None, int_funcs=None, mismatch=None):
        df = self.root.data
        cols = parse_columns(df, cols)
        columns_data_types = df.cols.data_types(tidy=False)

        def value_counts(series):
            return series.value_counts()

        delayed_results = []

        for col_name in cols:
            a = df.map_partitions(lambda df: df[col_name].apply(
                lambda row: Infer.parse((col_name, row), infer, columns_data_types, str_funcs, int_funcs,
                                        full=False))).compute()

            f = df.functions.map_delayed(a, value_counts)
            delayed_results.append({col_name: f.to_dict()})

        results_compute = dask.compute(*delayed_results)
        result = {}

        # Convert list to dict
        for i in results_compute:
            result.update(i)

        if infer is True:
            result = fill_missing_var_types(result, columns_data_types)
        else:
            result = self.parse_inferred_types(result)

        return result

    def nest(self, cols="*", separator="", output_col=None, drop=False, shape="string"):
        """
        Merge multiple columns with the format specified
        :param cols: columns to be nested
        :param separator: char to be used as separator at the concat time
        :param shape: final data type, 'array', 'string' or 'vector'
        :param output_col:
        :return: Dask DataFrame
        """

        df = self.root
        cols = parse_columns(df, cols)
        # output_col = val_to_list(output_col)
        # check_column_numbers(cols, 2)
        if output_col is None:
            output_col = name_col(cols)
            # RaiseIt.type_error(output_col, ["str"])

        # output_col = parse_columns(df, output_col, accepts_missing_cols=True)

        output_ordered_columns = df.cols.names()

        def _nest_string(row):
            v = row[cols[0]].astype(str)
            for i in range(1, len(cols)):
                v = v + separator + row[cols[i]].astype(str)
            return v

        def _nest_array(row):
            # https://stackoverflow.com/questions/43898035/pandas-combine-column-values-into-a-list-in-a-new-column/43898233
            # t['combined'] = t.values.tolist()

            v = row[cols[0]].astype(str)
            for i in range(1, len(cols)):
                v += ", " + row[cols[i]].astype(str)
            return "[" + v + "]"

        if shape == "string":
            kw_columns = {output_col: _nest_string}
        else:
            kw_columns = {output_col: _nest_array}

        dfd = df.cols.assign(kw_columns).data

        if output_col not in output_ordered_columns:
            col_index = output_ordered_columns.index(cols[-1]) + 1
            output_ordered_columns[col_index:col_index] = [output_col]

        meta = Meta.action(df.meta, Actions.NEST.value, list(kw_columns.values()))

        if drop is True:
            for input_col in cols:
                if input_col in output_ordered_columns and input_col != output_col:
                    output_ordered_columns.remove(input_col)

        return self.root.new(dfd, meta).cols.select(output_ordered_columns)
