import builtins
import re
import unicodedata

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.dataframe import from_delayed
from dask.dataframe.core import DataFrame
from dask_ml.impute import SimpleImputer
from multipledispatch import dispatch
from sklearn.preprocessing import MinMaxScaler

from optimus.engines.base.columns import BaseColumns
from optimus.engines.dask.ml.encoding import index_to_string as ml_index_to_string
from optimus.engines.dask.ml.encoding import string_to_index as ml_string_to_index
from optimus.helpers.check import equal_function, is_pandas_series
from optimus.helpers.columns import parse_columns, validate_columns_names, check_column_numbers, get_output_cols
from optimus.helpers.constants import RELATIVE_ERROR
from optimus.helpers.converter import format_dict, cudf_series_to_pandas
from optimus.helpers.core import val_to_list
from optimus.helpers.parser import compress_dict
from optimus.infer import Infer, is_list, is_list_of_tuples, is_one_element, is_int
from optimus.infer import is_
from optimus.profiler.functions import fill_missing_var_types


# This implementation works for Dask and dask_cudf

class DaskBaseColumns(BaseColumns):

    def __init__(self, df):
        super(DaskBaseColumns, self).__init__(df)

    def frequency(self, columns, n=10, percentage=False, total_rows=None):
        df = self.df
        columns = parse_columns(df, columns)
        result = {}
        lazy_results = [df[col_name].value_counts().nlargest(n) for col_name in columns]
        temp_result = dd.compute(*lazy_results)

        for temp_result_col in temp_result:
            if not is_pandas_series(temp_result_col):
                temp_result_col = cudf_series_to_pandas(temp_result_col)
            for i, j in temp_result_col.iteritems():
                result.setdefault(temp_result_col.name, []).append({"value": i, "count": j})

        if percentage is True:
            if total_rows is None:
                total_rows = df.rows.count()
            for value_counts in result.values():
                for value_count in value_counts:
                    value_count["percentage"] = round((value_count["count"] * 100 / total_rows), 2)

        return result

    @staticmethod
    def mode(columns):
        pass

    @staticmethod
    def abs(columns):
        pass

    @staticmethod
    def get_meta(col_name, spec=None):
        pass

    @staticmethod
    def set_meta(col_name, spec=None, value=None, missing=dict):
        pass

    @staticmethod
    def bucketizer(input_cols, splits, output_cols=None):
        pass

    def index_to_string(self, input_cols=None, output_cols=None, columns=None):
        df = self.df

        df = ml_index_to_string(df, input_cols, output_cols, columns)

        return df

    def string_to_index(self, input_cols=None, output_cols=None, columns=None):

        """
        Encodes a string column of labels to a column of label indices
        :param input_cols:
        :param output_cols:
        :param columns:
        :return:
        """
        df = self.df

        df = ml_string_to_index(df, input_cols, output_cols, columns)

        return df

    @staticmethod
    def values_to_cols(input_cols):
        pass

    def clip(self, columns, lower_bound, upper_bound):
        df = self.df
        columns = parse_columns(df, columns)
        df[columns] = df[columns].clip(lower_bound, upper_bound)

        return df

    def qcut(self, columns, num_buckets, handle_invalid="skip"):
        #
        # df = self.df
        # columns = parse_columns(df, columns)
        # df[columns] = df[columns].map_partitions(pd.cut, num_buckets)
        # return df
        pass

    @staticmethod
    def boxplot(columns):
        pass

    @staticmethod
    def correlation(input_cols, method="pearson", output="json"):
        pass

    @staticmethod
    def count_mismatch(columns_mismatch: dict = None):
        pass

    def count(self):
        df = self.df
        return len(df)

    @staticmethod
    def frequency_by_group(columns, n=10, percentage=False, total_rows=None):
        pass

    @staticmethod
    def scatter(columns, buckets=10):
        pass

    @staticmethod
    def cell(column):
        pass

    def iqr(self, columns, more=None, relative_error=RELATIVE_ERROR):
        """
        Return the column Inter Quartile Range
        :param columns:
        :param more: Return info about q1 and q3
        :param relative_error:
        :return:
        """
        df = self.df
        iqr_result = {}
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        quartile = df.cols.percentile(columns, [0.25, 0.5, 0.75], relative_error=relative_error)
        # print(quartile)
        for col_name in columns:

            q1 = quartile[col_name]["percentile"]["0.25"]
            q2 = quartile[col_name]["percentile"]["0.5"]
            q3 = quartile[col_name]["percentile"]["0.75"]

            iqr_value = q3 - q1
            if more:
                result = {"iqr": iqr_value, "q1": q1, "q2": q2, "q3": q3}
            else:
                result = iqr_value
            iqr_result[col_name] = result

        return format_dict(iqr_result)

    @staticmethod
    def standard_scaler():
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

    def z_score(self, input_cols, output_cols=None):
        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        for input_col, output_col in zip(input_cols, output_cols):
            df[output_col] = (df[input_col] - df[input_col].mean()) / df[input_col].std(ddof=0)
        return df

    @staticmethod
    def _math(columns, operator, new_column):
        pass

    @staticmethod
    def select_by_dtypes(data_type):
        pass

    @staticmethod
    def nunique(*args, **kwargs):
        pass

    def unique(self, columns):
        df = self.df
        return df.drop_duplicates()

    def value_counts(self, columns):
        """
        Return the counts of uniques values
        :param columns:
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        # .value(columns, 1)

        result = {}
        for col_name in columns:
            result.update(compress_dict(df[col_name].value_counts().ext.to_dict(), col_name))
        return result

    def extract(self, input_cols, output_cols, regex):
        df = self.df
        from optimus.engines.base.dataframe.commons import extract
        df = extract(df, input_cols, output_cols, regex)
        return df

    def slice(self, input_cols, output_cols, start, stop, step):
        df = self.df

        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        for input_col, output_col in zip(input_cols, output_cols):
            df[output_col] = df[input_col].str.slice(start, stop, step)
        return df

    def count_na(self, columns):
        """
        Return the NAN and Null count in a Column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        df = self.df
        return self.agg_exprs(columns, df.functions.count_na_agg, self)

    def count_zeros(self, columns):
        """
        Count zeros in a column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        df = self.df
        return self.agg_exprs(columns, df.functions.zeros_agg)

    def count_uniques(self, columns, estimate=True):
        """
        Return how many unique items exist in a columns
        :param columns: '*', list of columns names or a single column name.
        :param estimate: If true use HyperLogLog to estimate distinct count. If False use full distinct
        :type estimate: bool
        :return:
        """
        df = self.df
        return self.agg_exprs(columns, df.functions.count_uniques_agg, estimate)

    def is_na(self, input_cols, output_cols=None):
        """
        Replace null values with True and non null with False
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _is_na(value):
            return value is np.NaN

        df = self.df

        return df.cols.apply(input_cols, _is_na, output_cols=output_cols)

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

        df = self.df
        imputer = SimpleImputer(strategy=strategy, copy=False)

        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        _df = df[input_cols]
        imputer.fit(_df)
        # df[output_cols] = imputer.transform(_df)[input_cols]
        df[output_cols] = imputer.transform(_df)[input_cols]
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

    @staticmethod
    def years_between(input_cols, date_format=None, output_cols=None):
        pass

    def remove_white_spaces(self, input_cols, output_cols=None):

        def _remove_white_spaces(value, args):
            return value.str.replace(" ", "")

        df = self.df
        return df.cols.apply(input_cols, _remove_white_spaces, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols)

    def remove_special_chars(self, input_cols, output_cols=None):
        def _remove_special_chars(value, args):
            return value.str.replace('[^A-Za-z0-9]+', '')
            # return re.sub('[^A-Za-z0-9]+', '', value)

        df = self.df
        return df.cols.apply(input_cols, _remove_special_chars, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols)

    def remove_accents(self, input_cols, output_cols=None):
        def _remove_accents(value, args):
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

    def remove(self, input_cols, search=None, search_by="chars", output_cols=None):
        return self.replace(input_cols=input_cols, search=search, replace_by="", search_by=search_by,
                            output_cols=output_cols)

    def reverse(self, input_cols, output_cols=None):
        def _reverse(value):
            return str(value)[::-1]

        df = self.df
        return df.cols.apply(input_cols, _reverse, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols)

    def drop(self, columns=None, regex=None, data_type=None):
        """
        Drop a list of columns
        :param columns: Columns to be dropped
        :param regex: Regex expression to select the columns
        :param data_type:
        :return:
        """
        df = self.df
        # if regex:
        #     r = re.compile(regex)
        #     columns = [c for c in list(df.columns) if re.match(regex, c)]
        #
        # columns = parse_columns(df, columns, filter_by_column_dtypes=data_type)
        # check_column_numbers(columns, "*")

        names = df.cols.names(columns, regex, data_type)
        df = df.drop(names, axis=1)

        df = df.meta.preserve(df, "drop", columns)

        return df

    @staticmethod
    def sort(order="asc", columns=None):
        pass

    def keep(self, columns=None, regex=None):
        """
        Drop a list of columns
        :param columns: Columns to be dropped
        :param regex: Regex expression to select the columns
        :param data_type:
        :return:
        """
        df = self.df
        if regex:
            r = re.compile(regex)
            columns = [c for c in list(df.columns) if re.match(regex, c)]

        columns = parse_columns(df, columns)
        check_column_numbers(columns, "*")

        df = df.drop(columns=list(set(df.columns) - set(columns)))

        df = df.meta.action("keep", columns)

        return df

    @staticmethod
    def move(column, position, ref_col=None):
        pass

    @staticmethod
    def astype(*args, **kwargs):
        pass

    def set(self, output_col, value=None):
        """
        Execute a hive expression. Also handle ints and list in columns
        :param output_col: Output columns
        :param value: numeric, list or hive expression
        :return:
        """

        df = self.df

        columns = parse_columns(df, output_col, accepts_missing_cols=True)
        check_column_numbers(columns, 1)

        if is_list(value):
            df = df.assign(**{output_col: np.array(value)})
        else:
            df = df.assign(**{output_col: value})

        return df

    @dispatch(list)
    def copy(self, columns) -> DataFrame:
        return self.copy(columns=columns)

    def copy(self, input_cols=None, output_cols=None, columns=None) -> DataFrame:
        """
        Copy one or multiple columns
        :param input_cols: Source column to be copied
        :param output_cols: Destination column
        :param columns: tuple of column [('column1','column_copy')('column1','column1_copy')()]
        :return:
        """
        df = self.df

        if columns is None:
            input_cols = parse_columns(df, input_cols)
            if is_list(input_cols) or is_one_element(input_cols):
                output_cols = get_output_cols(input_cols, output_cols)
        else:
            input_cols = list([c[0] for c in columns])
            output_cols = list([c[1] for c in columns])
            output_cols = get_output_cols(input_cols, output_cols)

        df = df.assign(**{output_col: df[input_col] for input_col, output_col in zip(input_cols, output_cols)})

        return df

    @staticmethod
    def apply_by_dtypes(columns, func, func_return_type, args=None, func_type=None, data_type=None):
        pass

    @staticmethod
    def apply_expr(input_cols, func=None, args=None, filter_col_by_dtypes=None, output_cols=None, meta=None):
        pass

    @staticmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        pass

    @staticmethod
    def append(dfs) -> DataFrame:
        """

        :param dfs:
        :return:
        """
        # df = dd.concat([self, dfs], axis=1)
        raise NotImplementedError
        # return df

    @staticmethod
    def exec_agg(exprs):
        """
        Execute and aggregation
        :param exprs:
        :return:
        """

        # 'scheduler' param values
        # "threads": a scheduler backed by a thread pool
        # "processes": a scheduler backed by a process pool (preferred option on local machines as it uses all CPUs)
        # "single-threaded" (aka “sync”): a synchronous scheduler, good for debugging

        # import dask.multiprocessing
        # import dask.threaded
        #
        # >> > dmaster.compute(get=dask.threaded.get)  # this is default for dask.dataframe
        # >> > dmaster.compute(get=dask.multiprocessing.get)  # try processes instead
        #
        agg_results = dask.compute(*exprs)
        result = {}

        # Parsing results
        def parse_percentile(_value):
            _result = {}
            if is_(_value, pd.core.series.Series):
                _result.setdefault(_value.name, {str(i): j for i, j in dict(_value).items()})
            else:
                for (p_col_name, p_result) in _value.iteritems():
                    if is_(p_result, pd.core.series.Series):
                        p_result = dict(p_result)
                    _result.setdefault(p_col_name, {str(i): j for i, j in p_result.items()})
            return _result

        def parse_hist(_value):
            _result = {}
            for _col_name, values in _value.items():
                _hist = []

                x = values["count"]
                y = values["bins"]
                for idx, v in enumerate(y):
                    if idx < len(y) - 1:
                        _hist.append({"count": x[idx], "lower": y[idx], "upper": y[idx + 1]})
                _result.setdefault(_col_name, _hist)
            return _result

        def parse_count_uniques(_value):
            # Because count_uniques() return and Scalar and not support casting we need to make
            # the cast to int after compute()
            # Reference https://github.com/dask/dask/issues/1445
            return {_col_name: int(values) for _col_name, values in _value.items()}

        for agg_name, col_name_result in agg_results:
            if agg_name == "percentile":
                col_name_result = parse_percentile(col_name_result)
            elif agg_name == "hist":
                col_name_result = parse_hist(col_name_result)
            elif agg_name == "count_uniques":
                col_name_result = parse_count_uniques(col_name_result)

            # Process by datatype
            if is_(col_name_result, pd.core.series.Series):
                # col_name_result = pd.Series(col_name_result)
                # print("COL NAME RESULT",col_name_result)
                index = col_name_result.index
                for cols_name in index:
                    result.setdefault(cols_name, {}).update({agg_name: col_name_result[cols_name]})
            else:
                index = col_name_result
                for col_name, value in index.items():
                    result.setdefault(col_name, {}).update({agg_name: col_name_result[col_name]})

        return result

    def create_exprs(self, columns, funcs, *args):

        df = self.df
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
            # print("FUNC", func)
            # Create expression for functions that accepts multiple columns
            filtered_column = []
            for col_name in columns:
                # If the key exist update it
                if not _filter(col_name, func):
                    filtered_column.append(col_name)
            if len(filtered_column) > 0:
                # print("ITER", col_name)
                func_result = func(columns, args)(df)
                for k, v in func_result.items():
                    result[k] = {}
                    result[k] = v
        result = list(result.items())

        return result

    # TODO: Check if we must use * to select all the columns
    @dispatch(object, object)
    def rename(self, columns_old_new=None, func=None):
        """"
        Changes the name of a column(s) dataFrame.
        :param columns_old_new: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
        :param func: can be lower, upper or any string transformation function
        """

        df = self.df

        # Apply a transformation function
        if is_list_of_tuples(columns_old_new):
            validate_columns_names(df, columns_old_new)
            for col_name in columns_old_new:

                old_col_name = col_name[0]
                if is_int(old_col_name):
                    old_col_name = df.schema.names[old_col_name]
                if func:
                    old_col_name = func(old_col_name)

                current_meta = df.meta.get()
                # DaskColumns.set_meta(col_name, "optimus.transformations", "rename", append=True)
                # TODO: this seems to the only change in this function compare to pandas. Maybe this can be moved to a base class

                new_column = col_name[1]
                if old_col_name != col_name:
                    df = df.rename(columns={old_col_name: new_column})

                df = df.meta.preserve(df, value=current_meta)

                df = df.meta.rename((old_col_name, new_column))

        return df

    @dispatch(list)
    def rename(self, columns_old_new=None):
        return self.rename(columns_old_new, None)

    @dispatch(object)
    def rename(self, func=None):
        return self.rename(None, func)

    @dispatch(str, str, object)
    def rename(self, old_column, new_column, func=None):
        return self.rename([(old_column, new_column)], func)

    @dispatch(str, str)
    def rename(self, old_column, new_column):
        return self.rename([(old_column, new_column)], None)

    @staticmethod
    def date_transform(input_cols, current_format=None, output_format=None, output_cols=None):
        raise NotImplementedError('Look at me I am dask now')

    def fill_na(self, input_cols, value=None, output_cols=None):
        """
        Replace null data with a specified value
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param value: value to replace the nan/None values
        :return:
        """

        def _fill_na(value, new_value):
            return new_value if value is np.NaN else value

        df = self.df

        return df.cols.apply(input_cols, _fill_na, args=value, output_cols=output_cols)

    def count_by_dtypes(self, columns, infer=False, str_funcs=None, int_funcs=None, mismatch=None):
        df = self.df
        columns = parse_columns(df, columns)
        columns_dtypes = df.cols.dtypes()

        def value_counts(series):
            return series.value_counts()

        delayed_results = []

        for col_name in columns:
            a = df.map_partitions(lambda df: df[col_name].apply(
                lambda row: Infer.parse((col_name, row), infer, columns_dtypes, str_funcs, int_funcs,
                                        full=False))).compute()

            f = df.functions.map_delayed(a, value_counts)
            delayed_results.append({col_name: f.to_dict()})

        results_compute = dask.compute(*delayed_results)
        result = {}

        # Convert list to dict
        for i in results_compute:
            result.update(i)

        if infer is True:
            result = fill_missing_var_types(result, columns_dtypes)
        else:
            result = self.parse_profiler_dtypes(result)

        return result

    def lower(self, input_cols, output_cols=None):
        def _lower(value, args):
            return value.str.lower()

        df = self.df
        return df.cols.apply(input_cols, _lower, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols)

    def upper(self, input_cols, output_cols=None):
        def _upper(value, args):
            return value.str.upper()

        df = self.df
        return df.cols.apply(input_cols, _upper, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols)

    def trim(self, input_cols, output_cols=None):

        def _trim(value, args):
            return value.str.strip()

        df = self.df
        return df.cols.apply(input_cols, _trim, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols)

    def apply(self, input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
              filter_col_by_dtypes=None, output_cols=None, skip_output_cols_processing=False, meta="apply"):

        df = self.df

        input_cols = parse_columns(df, input_cols, filter_by_column_dtypes=filter_col_by_dtypes,
                                   accepts_missing_cols=True)
        check_column_numbers(input_cols, "*")

        if skip_output_cols_processing:
            output_cols = val_to_list(output_cols)
        else:
            output_cols = get_output_cols(input_cols, output_cols)

        if output_cols is None:
            output_cols = input_cols

        # args = val_to_list(args)

        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        result = {}

        for input_col, output_col in zip(input_cols, output_cols):
            partitions = df[input_col].to_delayed()
            # print("aAA3", args)
            temp = [dask.delayed(func)(part, args)
                    for part in partitions]
            result[output_col] = from_delayed(temp)

        return df.assign(**result)

    # TODO: Maybe should be possible to cast and array of integer for example to array of double
    def cast(self, input_cols=None, dtype=None, output_cols=None, columns=None):
        """
        Cast a column or a list of columns to a specific data type
        :param input_cols: Columns names to be casted
        :param output_cols:
        :param dtype: final data type
        :param columns: List of tuples of column names and types to be casted. This variable should have the
                following structure:
                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]
                The first parameter in each tuple is the column name, the second is the final datatype of column after
                the transformation is made.
        :return: Dask DataFrame
        """

        df = self.df
        _dtypes = []

        def _cast_int(value):
            try:
                return int(value)
            except ValueError:
                return None

        def _cast_float(value):
            try:
                return float(value)
            except ValueError:
                return None

        def _cast_bool(value):
            if value is None:
                return None
            else:
                return bool(value)

        def _cast_str(value):
            try:
                return value.astype(str)
            except:
                return str(value)

        # Parse params
        if columns is None:
            input_cols = parse_columns(df, input_cols)
            if is_list(input_cols) or is_one_element(input_cols):
                output_cols = get_output_cols(input_cols, output_cols)
                for _ in builtins.range(0, len(input_cols)):
                    _dtypes.append(dtype)
        else:
            input_cols = list([c[0] for c in columns])
            if len(columns[0]) == 2:
                output_cols = get_output_cols(input_cols, output_cols)
                _dtypes = list([c[1] for c in columns])
            elif len(columns[0]) == 3:
                output_cols = list([c[1] for c in columns])
                _dtypes = list([c[2] for c in columns])

            output_cols = get_output_cols(input_cols, output_cols)

        for input_col, output_col, dtype in zip(input_cols, output_cols, _dtypes):

            if dtype == 'int':
                func = _cast_int
            elif dtype == 'float':
                func = _cast_float
            elif dtype == 'bool':
                func = _cast_bool
            else:
                func = _cast_str

            # df.cols.apply(input_col, func=func, func_return_type=dtype, output_cols=output_col)
            # df[output_col] = df[input_col].apply(func=_cast_str, meta=df[input_col])
            df[output_col] = df[input_col].astype(dtype)

            df[output_col].odtype = dtype

        return df

    def nest(self, input_cols, shape="string", separator="", output_col=None):
        """
        Concat multiple columns to one with the format specified
        :param input_cols: columns to be nested
        :param separator: char to be used as separator at the concat time
        :param shape: final data type, 'array', 'string' or 'vector'
        :param output_col:
        :return: Dask DataFrame
        """

        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_col = parse_columns(df, output_col, accepts_missing_cols=True)
        check_column_numbers(output_col, 1)

        def _nest_string(row):
            v = row[input_cols[0]].astype(str)
            for i in builtins.range(1, len(input_cols)):
                v = v + separator + row[input_cols[i]].astype(str)
            return v

        def _nest_array(row):
            v = row[input_cols[0]].astype(str)
            for i in builtins.range(1, len(input_cols)):
                v += ", " + row[input_cols[i]].astype(str)
            return "[" + v + "]"

        if shape == "string":
            kw_columns = {output_col[0]: _nest_string}
        else:
            kw_columns = {output_col[0]: _nest_array}
        df = df.assign(**kw_columns)
        return df

    def unnest(self, input_cols, separator=None, splits=0, index=None, output_cols=None, drop=False):

        """
        Split an array or string in different columns
        :param input_cols: Columns to be un-nested
        :param output_cols: Resulted on or multiple columns  after the unnest operation [(output_col_1_1,output_col_1_2), (output_col_2_1, output_col_2]
        :param separator: char or regex
        :param splits: Number of rows to un-nested. Because we can not know beforehand the number of splits
        :param index: Return a specific index per columns. [{1,2},()]
        :param drop:
        """
        # Special case. A dot must be escaped
        if separator == ".":
            separator = "\\."

        df = self.df

        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        # new data frame with split value columns

        for input_col in input_cols:
            df_new = df[input_col].astype("str").str.split(separator, n=splits)
            # Sometime when split the result are less parts that the split param. We handle this returning Null
            kw_columns = {output_cols[0] + "_" + str(i): df_new.map(lambda x, i=i: x[i] if i < len(x) else None) for i
                          in range(splits)}
            df = df.assign(**kw_columns)

        if drop is True:
            df.drop(columns=input_cols, inplace=True)

        return df

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
            regex = re.compile("|".join(map(re.escape, search)))
        elif search_by == "words":
            regex = (r'\b%s\b' % r'\b|\b'.join(map(re.escape, search)))
        else:
            regex = search

        def _remove_white_spaces(value, args):
            return value.str.replace(args[0], args[1])

        df.cols.apply(input_cols, _remove_white_spaces, func_return_type=str,
                      filter_col_by_dtypes=df.constants.STRING_TYPES,
                      output_cols=output_cols, args=(regex, replace_by))

        return df

    def is_numeric(self, col_name):
        """
        Check if a column is numeric
        :param col_name:
        :return:
        """
        df = self.df
        # TODO: Check if this is the best way to check the data type
        if np.dtype(df[col_name]).type in [np.int64, np.int32, np.float64]:
            result = True
        else:
            result = False
        return result

    def schema_dtype(self, columns="*"):
        """
        Return the column(s) data type as Type
        :param columns: Columns to be processed
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        return format_dict([np.dtype(df[col_name]).type for col_name in columns])

    def select(self, columns="*", regex=None, data_type=None, invert=False):
        """
        Select columns using index, column name, regex to data type
        :param columns:
        :param regex: Regular expression to filter the columns
        :param data_type: Data type to be filtered for
        :param invert: Invert the selection
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns, is_regex=regex, filter_by_column_dtypes=data_type, invert=invert)
        if columns is not None:
            df = df[columns]
            # Metadata get lost when using select(). So we copy here again.
            # df.ext.meta = self.ext.meta
            result = df
        else:
            result = None

        return result
