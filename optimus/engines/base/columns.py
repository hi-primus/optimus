import re
import string
from abc import abstractmethod, ABC
from enum import Enum
from functools import reduce

import dask
import numpy as np
from dask.dataframe import from_delayed
from glom import glom

from optimus import functions as F
# from optimus.engines.base.dask.columns import TOTAL_PREVIEW_ROWS
from optimus.functions import to_numeric
from optimus.helpers.check import is_dask_dataframe, is_dask_cudf_dataframe
from optimus.helpers.columns import parse_columns, check_column_numbers, prepare_columns, get_output_cols
from optimus.helpers.constants import RELATIVE_ERROR, ProfilerDataTypes, Actions
from optimus.helpers.converter import format_dict
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.parser import parse_dtypes
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_dict, Infer, profiler_dtype_func, is_list, is_one_element, is_list_of_tuples


class BaseColumns(ABC):
    """Base class for all Cols implementations"""

    def __init__(self, df):
        self.df = df

    def append(self, dfs):
        """

        :param dfs:
        :return:
        """

        df = self.df
        df = dd.concat([dfs.reset_index(drop=True), df.reset_index(drop=True)], axis=1)
        return df

    def select(self, columns="*", regex=None, data_type=None, invert=False, accepts_missing_cols=False):
        """
        Select columns using index, column name, regex to data type
        :param columns:
        :param regex: Regular expression to filter the columns
        :param data_type: Data type to be filtered for
        :param invert: Invert the selection
        :param accepts_missing_cols:
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns, is_regex=regex, filter_by_column_dtypes=data_type, invert=invert,
                                accepts_missing_cols=accepts_missing_cols)
        if columns is not None:
            df = df[columns]
            result = df
        else:
            result = None

        return result

    def copy(self, input_cols=None, output_cols=None, columns=None):
        """
        Copy one or multiple columns
        :param input_cols: Source column to be copied
        :param output_cols: Destination column
        :param columns: tuple of column [('column1','column_copy')('column1','column1_copy')()]
        :return:
        """
        df = self.df
        output_ordered_columns = df.cols.names()

        if columns is None:
            input_cols = parse_columns(df, input_cols)
            if is_list(input_cols) or is_one_element(input_cols):
                output_cols = get_output_cols(input_cols, output_cols)

        if columns:
            input_cols = list([c[0] for c in columns])
            output_cols = list([c[1] for c in columns])
            output_cols = get_output_cols(input_cols, output_cols)

        for input_col, output_col in zip(input_cols, output_cols):
            if input_col != output_col:
                col_index = output_ordered_columns.index(input_col) + 1
                output_ordered_columns[col_index:col_index] = [output_col]

        kw_columns = {}
        for input_col, output_col in zip(input_cols, output_cols):
            kw_columns[output_col] = df[input_col]
            df = df.meta.copy({input_col: output_col})
        df = df.assign(**kw_columns)
        return df.cols.select(output_ordered_columns)

    def drop(self, columns=None, regex=None, data_type=None):
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
            columns = [c for c in list(df.columns) if re.match(r, c)]

        columns = parse_columns(df, columns, filter_by_column_dtypes=data_type)
        check_column_numbers(columns, "*")

        df = df.drop(columns=columns)

        df = df.meta.preserve(df, Actions.DROP.value, columns)

        return df

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
            # r = re.compile(regex)
            columns = [c for c in list(df.columns) if re.match(regex, c)]

        columns = parse_columns(df, columns)
        check_column_numbers(columns, "*")

        df = df.drop(columns=list(set(df.columns) - set(columns)))

        df = df.meta.preserve(df, Actions.KEEP.value, columns)

        return df

    def word_count(self, input_cols, output_cols=None):
        """
        Count words by column element wise
        :param input_cols:
        :param output_cols:
        :return:
        """
        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)
        for input_col, output_col in zip(input_cols, output_cols):
            df[output_col] = df[input_col].str.split().str.len()
        return df

    @staticmethod
    @abstractmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        pass

    def apply(self, input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
              filter_col_by_dtypes=None, output_cols=None, skip_output_cols_processing=False,
              meta_action=Actions.APPLY_COLS.value, mode="pandas", set_index=False):

        df = self.df

        columns = prepare_columns(df, input_cols, output_cols, filter_by_column_dtypes=filter_col_by_dtypes,
                                  accepts_missing_cols=True)

        # check_column_numbers(input_cols, "*")

        kw_columns = {}
        output_ordered_columns = df.cols.names()

        for input_col, output_col in columns:
            if mode == "pandas" and (is_dask_dataframe(df) or is_dask_cudf_dataframe(df)):

                partitions = df.to_delayed()
                delayed_parts = [dask.delayed(func)(part[input_col], args)
                                 for part in partitions]

                kw_columns[output_col] = from_delayed(delayed_parts)

            elif mode == "vectorized" or mode == "pandas":
                kw_columns[output_col] = func(df[input_col], args)

            elif mode == "map":
                kw_columns[output_col] = df[input_col].map(func, args)

            # Preserve column order
            if output_col not in df.cols.names():
                col_index = output_ordered_columns.index(input_col) + 1
                output_ordered_columns[col_index:col_index] = [output_col]

            # Preserve actions for the profiler
            df = df.meta.preserve(df, meta_action, output_col)

        if set_index is True:
            df = df.reset_index()

        df = df.assign(**kw_columns)
        df = df.cols.select(output_ordered_columns)

        return df

    @staticmethod
    @abstractmethod
    def apply_by_dtypes(columns, func, func_return_type, args=None, func_type=None, data_type=None):
        pass

    @staticmethod
    @abstractmethod
    def set(output_col, value=None):
        pass

    @staticmethod
    @abstractmethod
    def rename(*args, **kwargs) -> Enum:
        pass

    def parse_profiler_dtypes(self, col_data_type):
        """
        Parse a spark data type to a profiler data type
        :return:
        """
        df = self.df
        columns = {}
        for k, v in col_data_type.items():
            # Initialize values to 0
            result_default = {data_type: 0 for data_type in df.constants.DTYPES_TO_PROFILER.keys()}
            for k1, v1 in v.items():
                for k2, v2 in df.constants.DTYPES_TO_PROFILER.items():
                    if k1 in df.constants.DTYPES_TO_PROFILER[k2]:
                        result_default[k2] = result_default[k2] + v1
            columns[k] = result_default
        return columns

    def profiler_dtypes(self, columns="*"):
        """
        Get the profiler data types from the meta data
        :param columns:
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        result = {}
        for col_name in columns:
            column_meta = glom(df.meta.get(), f"profile.columns.{col_name}", skip_exc=KeyError)
            if column_meta is None:
                result[col_name] = None
            else:
                result[col_name] = column_meta["profiler_dtype"]
        return result

    def set_profiler_dtypes(self, columns: dict):
        """
        Set profiler data type
        :param columns: A dict with the form {"col_name": profiler dtype}
        :return:
        """
        df = self.df
        for col_name, dtype in columns.items():
            if dtype in ProfilerDataTypes.list():
                df.meta.set(f"profile.columns.{col_name}.profiler_dtype", dtype)
                df.meta.preserve(df, Actions.PROFILER_DTYPE.value, col_name)
            else:
                RaiseIt.value_error(dtype, ProfilerDataTypes.list())

    @staticmethod
    @abstractmethod
    def cast(input_cols=None, dtype=None, output_cols=None, columns=None):
        pass

    @staticmethod
    @abstractmethod
    def astype(*args, **kwargs):
        pass

    def round(self, input_cols, decimals=1, output_cols=None):
        """

        :param input_cols:
        :param decimals:
        :param output_cols:
        :return:
        """
        df = self.df
        columns = prepare_columns(df, input_cols, output_cols)
        for input_col, output_col in columns:
            df[output_col] = df[input_col].round(decimals)
        return df

    def ceil(self, input_cols, output_cols=None):
        """

        :param input_cols:
        :param output_cols:
        :return:
        """
        df = self.df
        columns = prepare_columns(df, input_cols, output_cols)
        for input_col, output_col in columns:
            df[output_col] = df[input_col].map(np.ceil)
        return df

    def floor(self, input_cols, output_cols=None):
        """

        :param input_cols:
        :param output_cols:
        :return:
        """
        df = self.df
        columns = prepare_columns(df, input_cols, output_cols)
        for input_col, output_col in columns:
            df[output_col] = df[input_col].map(np.floor)
        return df

    def patterns(self, input_cols, output_cols=None, mode=0):
        """
        Replace alphanumeric and punctuation chars for canned chars. We aim to help to find string patterns
        c = Any alpha char in lower or upper case form
        l = Any alpha char in lower case
        U = Any alpha char in upper case
        * = Any alphanumeric in lower or upper case
        ! = Any punctuation

        :param input_cols:
        :param output_cols:
        :param mode:
        0: Identify lower, upper, digits. Except spaces and special chars.
        1: Identify chars, digits. Except spaces and special chars
        2: Identify Any alphanumeric. Except spaces and special chars
        3: Identify alphanumeric and special chars. Except white spaces
        :return:
        """
        df = self.df

        def split(word):
            return [char for char in word]

        alpha_lower = split(string.ascii_lowercase)
        alpha_upper = split(string.ascii_uppercase)
        digits = split(string.digits)
        punctuation = split(string.punctuation)

        if mode == 0:
            search_by = alpha_lower + alpha_upper + digits
            replace_by = ["l"] * len(alpha_lower) + ["U"] * len(alpha_upper) + ["#"] * len(digits)
        elif mode == 1:
            search_by = alpha_lower + alpha_upper + digits
            replace_by = ["c"] * len(alpha_lower) + ["c"] * len(alpha_upper) + ["#"] * len(digits)
        elif mode == 2:
            search_by = alpha_lower + alpha_upper + digits
            replace_by = ["*"] * len(alpha_lower + alpha_upper + digits)
        elif mode == 3:
            search_by = alpha_lower + alpha_upper + digits + punctuation
            replace_by = ["*"] * len(alpha_lower + alpha_upper + digits + punctuation)

        result = {}
        columns = prepare_columns(df, input_cols, output_cols)

        for input_col, output_col in columns:
            result[input_col] = df[input_col].str.replace(search_by,
                                                          replace_by).value_counts().to_pandas().to_dict()
        return result

    def groupby(self, by, agg, order="asc", *args, **kwargs):
        """
        This helper function aims to help managing columns name in the aggregation output.
        Also how to handle ordering columns because dask can order columns
        :param by:
        :param agg:
        :param args:
        :param kwargs:
        :return:
        """
        df = self.df
        compact = {}
        for col_agg in list(agg.values()):
            for col_name, _agg in col_agg.items():
                compact.setdefault(col_name, []).append(_agg)

        df = df.groupby(by=by).agg(compact).reset_index()
        df.columns = (val_to_list(by) + val_to_list(list(agg.keys())))

        return df

    def join(self, df_right, how="left", on=None, left_on=None, right_on=None, order=True, *args, **kwargs):
        """
        Join 2 dataframes SQL style
        :param df_right:
        :param how{‘left’, ‘right’, ‘outer’, ‘inner’}, default ‘left’
        :param on:
        :param left_on:
        :param right_on:
        :param order: Order the columns putting the left df columns first then the key column and the right df columns
        :param args:
        :param kwargs:

        :return:
        """
        suffix_left = "_left"
        suffix_right = "_right"

        df_left = self.df

        if on is not None:
            left_on = on
            right_on = on

        if df_left.cols.dtypes(left_on) == "category":
            df_left[left_on] = df_left[left_on].cat.as_ordered()

        if df_right.cols.dtypes(right_on) == "category":
            df_right[right_on] = df_right[right_on].cat.as_ordered()

        # Join do not work with different data types.
        # Use set_index to return a index in the dataframe
        df_left[left_on] = df_left[left_on].astype(str)
        df_left.set_index(left_on)

        df_right[right_on] = df_right[right_on].astype(str)
        df_right.set_index(right_on)

        l_c = df_left.cols.names()[-1]
        # Use to reorder de output
        df_left = df_left.merge(df_right, how=how, on=on, left_on=left_on, right_on=right_on,
                                suffixes=(suffix_left, suffix_right))

        # Remove duplicated index if the name is the same. If the index name are not the same
        if order is True:
            if left_on != right_on:
                df_left = df_left.cols.drop(right_on)
            df_left = df_left.cols.move(left_on, "before", l_c)

        return df_left

    def is_match(self, columns, dtype, invert=False):
        """
        Find the rows that match a data type
        :param columns:
        :param dtype: data type to match
        :param invert: Invert the match
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)

        dtype = parse_dtypes(df, dtype)
        f = profiler_dtype_func(dtype)
        if f is not None:
            for col_name in columns:
                df = df[col_name].apply(f)
                df = ~df if invert is True else df
            return df

    def move(self, column, position, ref_col=None):
        """
        Move a column to specific position
        :param column: Column to be moved
        :param position: Column new position. Accepts 'after', 'before', 'beginning', 'end'
        :param ref_col: Column taken as reference
        :return: Spark DataFrame
        """
        print("columns", column)
        df = self.df
        # Check that column is a string or a list
        column = parse_columns(df, column)
        ref_col = parse_columns(df, ref_col)

        # Get dataframe columns
        all_columns = df.cols.names()

        # Get source and reference column index position
        new_index = all_columns.index(ref_col[0])

        # Column to move
        column_to_move_index = all_columns.index(column[0])

        if position == 'after':
            # Check if the movement is from right to left:
            if new_index < column_to_move_index:
                new_index = new_index + 1
        elif position == 'before':  # If position if before:
            if new_index >= column_to_move_index:  # Check if the movement if from right to left:
                new_index = new_index - 1
        elif position == 'beginning':
            new_index = 0
        elif position == 'end':
            new_index = len(all_columns)
        else:
            RaiseIt.value_error(position, ["after", "before", "beginning", "end"])

        # Move the column to the new place
        for col_name in column:
            print("col_name", col_name)
            all_columns.insert(new_index, all_columns.pop(all_columns.index(col_name)))  # insert and delete a element
            # new_index = new_index + 1
        return df[all_columns]

    def sort(self, order: [str, list] = "asc", columns=None):
        """
        Sort data frames columns asc or desc
        :param order: 'asc' or 'desc' accepted
        :param columns:
        :return: DataFrame
        """
        df = self.df
        if columns is None:
            _reverse = None
            if order == "asc":
                _reverse = False
            elif order == "desc":
                _reverse = True
            else:
                RaiseIt.value_error(order, ["asc", "desc"])

            columns = df.cols.names()
            columns.sort(key=lambda v: v.upper(), reverse=_reverse)

        return df.cols.select(columns)

    def dtypes(self, columns="*"):
        """
        Return the column(s) data type as string
        :param columns: Columns to be processed
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        data_types = ({k: str(v) for k, v in dict(df.dtypes).items()})
        return {col_name: data_types[col_name] for col_name in columns}

    def schema_dtype(self, columns="*"):
        """
        Return the column(s) data type as Type
        :param columns: Columns to be processed
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        result = {}
        for col_name in columns:
            if df[col_name].dtype.name == "category":
                result[col_name] = "category"
            else:
                result[col_name] = np.dtype(df[col_name]).type
        return format_dict(result)

    # @staticmethod
    # @abstractmethod
    # def create_exprs(columns, funcs, *args):
    #     pass

    def agg_exprs(self, columns, funcs, *args):
        """
        Create and run aggregation
        :param columns:
        :param funcs:
        :param args:
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)

        funcs = val_to_list(funcs)
        funcs = [func(df, columns, args) for func in funcs]
        # print("df[col_name][col_name]111[col_name]",type(df[columns][columns]))
        return df.cols.exec_agg(funcs[0])

    @staticmethod
    @abstractmethod
    def exec_agg(exprs):
        pass

    def mad(self, columns, relative_error=RELATIVE_ERROR, more=False):
        # df = self.df
        return self.agg_exprs(columns, F.mad, relative_error, more)

    def min(self, columns):
        df = self.df
        return df.cols.agg_exprs(columns, F.min)

    def mode(self, columns):
        df = self.df
        return df.cols.agg_exprs(columns, F.mode)

    def max(self, columns):

        return self.agg_exprs(columns, F.max)

    def range(self, columns):
        return self.agg_exprs(columns, F.range)

    def percentile(self, columns, values=None, relative_error=RELATIVE_ERROR):
        df = self.df

        if values is None:
            values = [0.5]
        return df.cols.agg_exprs(columns, F.percentile_agg, values, relative_error)

    def median(self, columns, relative_error=RELATIVE_ERROR):
        # return format_dict(self.percentile(columns, [0.5], relative_error))
        df = self.df
        return df.cols.agg_exprs(columns, F.percentile_agg, [0.5], relative_error)

        # Descriptive Analytics

    # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/

    @abstractmethod
    def kurtosis(self, columns):
        pass

    def mean(self, columns):
        df = self.df
        return df.cols.agg_exprs(columns, F.mean)

    @abstractmethod
    def skewness(self, columns):
        pass

    def sum(self, columns):
        df = self.df
        return df.cols.agg_exprs(columns, F.sum)

    def variance(self, columns):
        df = self.df
        return df.cols.agg_exprs(columns, F.variance)

    def std(self, columns):
        return self.agg_exprs(columns, F.std)

    def abs(self, input_cols, output_cols=None):
        """
        Apply abs to column
        :param input_cols:
        :param output_cols:
        :return:
        """

        def _abs(value, *args):
            return F.abs(value)

        df = self.df
        return df.cols.apply(input_cols, _abs, func_return_type=str,
                             output_cols=output_cols, meta_action=Actions.ABS.value, mode="vectorized")

    def extract(self, input_cols, output_cols=None):
        def _extract(value, *args):
            return F.extract(value, args)

        df = self.df
        return df.cols.apply(input_cols, _extract, func_return_type=str, filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, meta_action=Actions.EXTRACT.value, mode="vectorized")

    def slice(self, input_cols, start, stop, step, output_cols=None):
        def _slice(value, *args):
            return F.slice(value, start, stop, step)

        df = self.df
        return df.cols.apply(input_cols, _slice, func_return_type=str, filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, meta_action=Actions.SLICE.value, mode="vectorized")

    def lower(self, input_cols, output_cols=None):
        def _lower(value, *args):
            return F.lower(value)

        df = self.df
        return df.cols.apply(input_cols, _lower, func_return_type=str, filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, meta_action=Actions.LOWER.value, mode="vectorized")

    def upper(self, input_cols, output_cols=None):
        def _upper(value, *args):
            return F.upper(value)

        df = self.df
        return df.cols.apply(input_cols, _upper, func_return_type=str, filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, meta_action=Actions.UPPER.value, mode="vectorized")

    def trim(self, input_cols, output_cols=None):

        def _trim(value, *args):
            return F.trim(value)

        df = self.df
        return df.cols.apply(input_cols, _trim, func_return_type=str, filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, meta_action=Actions.TRIM.value, mode="vectorized")

    def date_format(self, input_cols, current_format=None, output_format=None, output_cols=None):

        def _date_format(value, args):
            _current_format = args[0]
            _output_format = args[1]
            return F.date_format(value, _current_format, _output_format)

        df = self.df
        return df.cols.apply(input_cols, _date_format, args=[current_format, output_format], func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, meta_action=Actions.DATE_FORMAT.value, mode="pandas")

    @staticmethod
    @abstractmethod
    def reverse(input_cols, output_cols=None):
        pass

    def remove(self, input_cols, search=None, search_by="chars", output_cols=None):
        return self.replace(input_cols=input_cols, search=search, replace_by="", search_by=search_by,
                            output_cols=output_cols)

    @staticmethod
    @abstractmethod
    def remove_accents(input_cols, output_cols=None):
        pass

    def remove_numbers(self, input_cols, output_cols=None):

        def _remove_numbers(value, args):
            return value.astype(str).str.replace(r'\d+', '')

        df = self.df
        return df.cols.apply(input_cols, _remove_numbers, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, mode="pandas", set_index=True)

    def remove_white_spaces(self, input_cols, output_cols=None):

        def _remove_white_spaces(value, args):
            return value.astype("str").str.replace(" ", "")

        df = self.df
        return df.cols.apply(input_cols, _remove_white_spaces, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, mode="pandas", set_index=True)

    def remove_special_chars(self, input_cols, output_cols=None):
        def _remove_special_chars(value, args):
            return value.astype(str).str.replace('[^A-Za-z0-9]+', '')

        df = self.df
        return df.cols.apply(input_cols, _remove_special_chars, func_return_type=str,
                             filter_col_by_dtypes=df.constants.STRING_TYPES,
                             output_cols=output_cols, mode="pandas", set_index=True)

    def year(self, input_cols, format=None, output_cols=None):
        """

        :param input_cols:
        :param output_cols:
        :param format:
        :return:
        """

        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        def func(_df, _input_col, _format):
            return _df._lib.to_datetime(_df[_input_col], format=_format).dt.year

        for input_col in input_cols:
            df = df.rows.apply(func, args=(input_col, format), output_cols=output_cols)

        return df

    def month(self, input_cols, format=None, output_cols=None):
        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        def func(_df, _input_col, _format):
            return _df.to_datetime(_df[_input_col], format=_format).dt.month

        return df.rows.apply(func, args=(one_list_to_val(input_cols), format), output_cols=output_cols)

    def day(self, input_cols, format=None, output_cols=None):
        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        def func(_df, _input_col, _format):
            return _df.to_datetime(_df[_input_col], format=_format).dt.day

        return df.rows.apply(func, args=(one_list_to_val(input_cols), format), output_cols=output_cols)

    def hour(self, input_cols, format=None, output_cols=None):
        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        def func(_df, _input_col, _format):
            return _df.to_datetime(_df[_input_col], format=_format).dt.hour

        return df.rows.apply(func, args=(one_list_to_val(input_cols), format), output_cols=output_cols)

    def minute(self, input_cols, format=None, output_cols=None):
        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        def func(_df, _input_col, _format):
            return _df.to_datetime(_df[_input_col], format=_format).dt.minute

        return df.rows.apply(func, args=(one_list_to_val(input_cols), format), output_cols=output_cols)

    def second(self, input_cols, format=None, output_cols=None):
        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        def func(_df, _input_col, _format):
            return _df.to_datetime(_df[_input_col], format=_format).dt.second

        return df.rows.apply(func, args=(one_list_to_val(input_cols), format), output_cols=output_cols)

    def weekday(self, input_cols, format=None, output_cols=None):
        df = self.df
        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        def func(_df, _input_col, _format):
            return _df.to_datetime(_df[_input_col], format=_format).dt.weekday

        return df.rows.apply(func, args=(one_list_to_val(input_cols), format), output_cols=output_cols)

    def years_between(self, input_cols, date_format=None, output_cols=None):
        df = self.df

        def _years_between(value, args):
            return F.years_between(value, *args)

        return df.cols.apply(input_cols, _years_between, args=[date_format], func_return_type=str,
                             output_cols=output_cols,
                             meta_action=Actions.YEARS_BETWEEN.value, mode="pandas", set_index=True)

    def replace(self, input_cols, search=None, replace_by=None, search_by="chars", ignore_case=False, output_cols=None):
        """
        Replace a value, list of values by a specified string
        :param input_cols: '*', list of columns names or a single column name.
        :param search: Values to look at to be replaced
        :param replace_by: New value to replace the old one
        :param search_by: Can be "full","words","chars" or "numeric".
        :param ignore_case: Ignore case when searching for match
        :param output_cols:
        :return: Dask DataFrame
        """

        df = self.df

        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)
        output_ordered_columns = df.cols.names()

        search = val_to_list(search)
        if search_by == "chars":
            # TODO: Maybe we could use replace_multi()
            str_regex = "|".join(map(re.escape, search))
        elif search_by == "words":
            str_regex = (r'\b%s\b' % r'\b|\b'.join(map(re.escape, search)))
        else:
            str_regex = (r'^\b%s\b$' % r'\b$|^\b'.join(map(re.escape, search)))

        if ignore_case is True:
            # Cudf do not accept re.compile as argument for replace
            # regex = re.compile(str_regex, re.IGNORECASE)
            regex = str_regex
        else:
            regex = str_regex

        kw_columns = {}
        for input_col, output_col in zip(input_cols, output_cols):
            # print("input_cols", regex, replace_by)
            # print("str_regex", str_regex)
            kw_columns[output_col] = df[input_col].astype(str).str.replace(str_regex, replace_by)

            if input_col != output_col:
                col_index = output_ordered_columns.index(input_col) + 1
                output_ordered_columns[col_index:col_index] = [output_col]

        df = df.assign(**kw_columns)
        # The apply function seems to have problem appending new columns https://github.com/dask/dask/issues/2690
        # df = df.cols.apply(input_cols, _replace, func_return_type=str,
        #                    filter_col_by_dtypes=df.constants.STRING_TYPES,
        #                    output_cols=output_cols, args=(regex, replace_by))

        df = df.meta.preserve(df, Actions.REPLACE.value, one_list_to_val(output_cols))
        return df.cols.select(output_ordered_columns)

    @staticmethod
    @abstractmethod
    def replace_regex(input_cols, regex=None, value=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def impute(input_cols, data_type="continuous", strategy="mean", output_cols=None):
        pass

    def fill_na(self, input_cols, value=None, output_cols=None):
        """
        Replace null data with a specified value
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param value: value to replace the nan/None values
        :return:
        """
        df = self.df

        def _fill_na(series, args):
            value = args
            return series.fillna(value)

        return df.cols.apply(input_cols, _fill_na, args=value, output_cols=output_cols, mode="vectorized")

    def is_na(self, input_cols, output_cols=None):
        """
        Replace null values with True and non null with False
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _is_na(value, args):
            return value.isnull()

        df = self.df

        return df.cols.apply(input_cols, _is_na, output_cols=output_cols, mode="vectorized")

    def count(self):
        df = self.df
        return len(df.cols.names())

    def count_na(self, columns):
        """
        Return the NAN and Null count in a Column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        df = self.df
        return df.cols.agg_exprs(columns, F.count_na)

    def unique(self, columns, values=None, relative_error=RELATIVE_ERROR):
        df = self.df

        return df.cols.agg_exprs(columns, F.unique, values, relative_error)

    def count_uniques(self, columns, values=None, relative_error=RELATIVE_ERROR):
        df = self.df

        return df.cols.agg_exprs(columns, F.count_uniques, values, relative_error)

    @staticmethod
    @abstractmethod
    def select_by_dtypes(data_type):
        pass

    def _math(self, columns, operator, output_col):

        """
        Helper to process arithmetic operation between columns. If a
        :param columns: Columns to be used to make the calculation
        :param operator: A lambda function
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        expr = reduce(operator, [to_numeric(df[col_name]).fillna(0) for col_name in columns])
        return df.assign(**{output_col: expr})

    def add(self, columns, output_col="sum"):
        """
        Add two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_col:
        :return:
        """
        df = self.df
        return df.cols._math(columns, lambda x, y: x + y, output_col)

    def sub(self, columns, output_col="sub"):
        """
        Subs two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_col:
        :return:
        """
        df = self.df
        return df.cols._math(columns, lambda x, y: x - y, output_col)

    def mul(self, columns, output_col="mul"):
        """
        Multiply two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_col:
        :return:
        """
        df = self.df
        return df.cols._math(columns, lambda x, y: x * y, output_col)

    def div(self, columns, output_col="div"):
        """
        Divide two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_col:
        :return:
        """
        df = self.df
        return df.cols._math(columns, lambda x, y: x / y, output_col)

    def z_score(self, input_cols, output_cols=None):

        df = self.df

        def _z_score(value, args):
            t = value.astype(float)
            return (t - t.mean()) / t.std(ddof=0)

        return df.cols.apply(input_cols, _z_score, func_return_type=float, output_cols=output_cols,
                             meta_action=Actions.Z_SCORE.value, mode="vectorized",
                             filter_col_by_dtypes=df.constants.NUMERIC_TYPES)

    @staticmethod
    @abstractmethod
    def min_max_scaler(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def standard_scaler(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def max_abs_scaler(input_cols, output_cols=None):
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
        for col_name in columns:

            q1 = quartile[col_name][0.25]
            q2 = quartile[col_name][0.5]
            q3 = quartile[col_name][0.75]

            iqr_value = q3 - q1
            if more:
                result = {"iqr": iqr_value, "q1": q1, "q2": q2, "q3": q3}
            else:
                result = iqr_value
            iqr_result[col_name] = result

        return format_dict(iqr_result)

    @staticmethod
    @abstractmethod
    def nest(input_cols, shape="string", separator="", output_col=None):
        pass

    def unnest(self, input_cols, separator=None, splits=2, index=None, output_cols=None, drop=False, mode="string"):

        """
        Split an array or string in different columns
        :param input_cols: Columns to be un-nested
        :param output_cols: Resulted on or multiple columns after the unnest operation [(output_col_1_1,output_col_1_2), (output_col_2_1, output_col_2]
        :param separator: char or regex
        :param splits: Number of columns splits.
        :param index: Return a specific index per columns. [1,2]
        :param drop:
        """
        df = self.df

        if separator is not None:
            separator = re.escape(separator)

        input_cols = parse_columns(df, input_cols)

        index = val_to_list(index)
        output_ordered_columns = df.cols.names()

        for idx, input_col in enumerate(input_cols):

            if is_list_of_tuples(index):
                final_index = index[idx]
            else:
                final_index = index

            if output_cols is None:
                final_columns = [input_col + "_" + str(i) for i in range(splits)]
            else:
                if is_list_of_tuples(output_cols):
                    final_columns = output_cols[idx]

                else:
                    final_columns = output_cols

            if mode == "string":
                df_new = df[input_col].astype(str).str.split(separator, expand=True, n=splits - 1)

            elif mode == "array":
                def func(value):
                    pdf = value.apply(pd.Series)
                    pdf.columns = final_columns
                    return pdf

                df_new = df[input_col].map_partitions(func, meta={c: object for c in final_columns})

            df_new.columns = final_columns
            if final_index:
                print("final_index", final_index[idx])
                df_new = df_new.cols.select(final_index[idx])
            df = df.cols.append(df_new)

        if drop is True:
            df = df.drop(columns=input_cols)
            for input_col in input_cols:
                if input_col in output_ordered_columns: output_ordered_columns.remove(input_col)

        df = df.meta.preserve(df, Actions.UNNEST.value, final_columns)

        # return df
        return df.cols.move(df_new.cols.names(), "after", input_cols)

    @staticmethod
    @abstractmethod
    def scatter(columns, buckets=10):
        pass

    def hist(self, columns, buckets=20):
        df = self.df
        result = self.agg_exprs(columns, df.functions.hist_agg, df, buckets, None)
        return result

    def cast_to_profiler_dtypes(self, input_col=None, dtype=None, columns=None):
        """
        Set a profiler datatype to a column an cast the column accordingly
        :param input_col:
        :param dtype:
        :param columns:
        :return:
        """
        df = self.df
        input_col = parse_columns(df, input_col)

        if not is_dict(columns):
            columns[input_col] = dtype

        # Map from profiler dtype to python dtype
        profiler_dtype_python = {ProfilerDataTypes.INT.value: "int",
                                 ProfilerDataTypes.DECIMAL.value: "float",
                                 ProfilerDataTypes.STRING.value: "object",
                                 ProfilerDataTypes.BOOLEAN.value: "bool",
                                 ProfilerDataTypes.DATE.value: "date",
                                 ProfilerDataTypes.ARRAY.value: "object",
                                 ProfilerDataTypes.OBJECT.value: "object",
                                 ProfilerDataTypes.GENDER.value: "object",
                                 ProfilerDataTypes.IP.value: "object",
                                 ProfilerDataTypes.URL.value: "object",
                                 ProfilerDataTypes.EMAIL.value: "object",
                                 ProfilerDataTypes.CREDIT_CARD_NUMBER.value: "object",
                                 ProfilerDataTypes.ZIP_CODE.value: "object"}
        # print("columns",columns)
        df = df.cols.cast(columns=columns)
        #
        # for col_name, _dtype in columns.items():
        #     python_dtype = profiler_dtype_python[_dtype]
        #     df.meta.set(f"profile.columns.{col_name}.profiler_dtype", _dtype)
        #
        #     df.meta.preserve(df, Actions.PROFILER_DTYPE.value, col_name)
        #
        #     #
        #     # # For categorical columns we need to transform the series to an object
        #     # # about doing arithmetical operation
        #     if df.cols.dtypes(col_name) == "category":
        #         df[col_name] = df[col_name].astype(object)
        #
        #
        #
        #
        #
        #     if _dtype == "date":
        #         # We can not use accesor .dt if the column datatype is not a date type
        #         # df[col_name] = df[col_name].astype('M8[us]')
        #         df.meta.set(f"profile.columns.{col_name}.profiler_dtype_fotmat", _dtype)
        #
        #     # print("_dtype",_dtype)
        return df

    @staticmethod
    @abstractmethod
    def count_mismatch(columns_mismatch: dict = None):
        """
        Result {'col_name': {'mismatch': 0, 'missing': 9, 'match': 0, 'profiler_dtype': 'object'}}
        :param columns_mismatch:
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def count_by_dtypes(columns, infer=False, str_funcs=None, int_funcs=None):
        pass

    def infer_profiler_dtypes(self, columns):
        """
        Infer datatypes from a sample
        :param columns:
        :return:Return a dict with the column and the inferred data type
        """
        df = self.df
        columns = parse_columns(df, columns)
        total_preview_rows = 30
        pdf = df.ext.head(columns, total_preview_rows).ext.to_pandas().applymap(Infer.parse_pandas)
        cols_and_inferred_dtype = {}
        for col_name in columns:
            _value_counts = pdf[col_name].value_counts()
            if _value_counts.index[0] != "null" and _value_counts.index[0] != "missing":
                r = _value_counts.index[0]
            elif _value_counts[0] < len(pdf):
                r = _value_counts.index[1]
            else:
                r = "object"
            cols_and_inferred_dtype[col_name] = r
        return cols_and_inferred_dtype

    @staticmethod
    @abstractmethod
    def frequency(columns, n=10, percentage=False, total_rows=None):
        """
        Result {'col_name': {'frequency': [{'value': 'LAUREY', 'count': 3},
           {'value': 'GARRIE', 'count': 3},
           {'value': 'ERIN', 'count': 2},
           {'value': 'DEVINDER', 'count': 1}],
          'count_uniques': 4}}
        :param columns:
        :param n:
        :param percentage:
        :param total_rows:
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def correlation(input_cols, method="pearson", output="json"):
        pass

    @staticmethod
    @abstractmethod
    def boxplot(columns):
        # """
        # Output values frequency in json format
        # :param columns: Columns to be processed
        # :return:
        # """
        # df = self
        # columns = parse_columns(df, columns)
        #
        # for col_name in columns:
        #     iqr = df.cols.iqr(col_name, more=True)
        #     lb = iqr["q1"] - (iqr["iqr"] * 1.5)
        #     ub = iqr["q3"] + (iqr["iqr"] * 1.5)
        #
        #     _mean = df.cols.mean(columns)
        #
        #     query = ((F.col(col_name) < lb) | (F.col(col_name) > ub))
        #     fliers = collect_as_list(df.rows.select(query).cols.select(col_name).limit(1000))
        #     stats = [{'mean': _mean, 'med': iqr["q2"], 'q1': iqr["q1"], 'q3': iqr["q3"], 'whislo': lb, 'whishi': ub,
        #               'fliers': fliers, 'label': one_list_to_val(col_name)}]
        #
        #     return stats
        pass

    def names(self, col_names="*", by_dtypes=None, invert=False):
        columns = parse_columns(self.df, col_names, filter_by_column_dtypes=by_dtypes, invert=invert)
        return columns

    def count_zeros(self, columns):
        df = self.df
        return df.cols.agg_exprs(columns, F.count_zeros)

    @staticmethod
    @abstractmethod
    def qcut(columns, num_buckets, handle_invalid="skip"):
        pass

    def clip(self, input_cols, lower_bound, upper_bound, output_cols=None):
        df = self.df

        def _clip(value, args):
            return F.clip(value, lower_bound, upper_bound)

        return df.cols.apply(input_cols, _clip, func_return_type=float, output_cols=output_cols,
                             meta_action=Actions.CLIP.value, mode="vectorized")

    @staticmethod
    @abstractmethod
    def string_to_index(input_cols=None, output_cols=None, columns=None):
        pass

    @staticmethod
    @abstractmethod
    def index_to_string(input_cols=None, output_cols=None, columns=None):
        pass

    @staticmethod
    @abstractmethod
    def bucketizer(input_cols, splits, output_cols=None):
        pass
