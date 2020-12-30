import re
import string
import time
from abc import abstractmethod, ABC
from functools import reduce

import dask
import numpy as np
import pandas as pd
import pydateinfer
from dask import dataframe as dd
from dask.dataframe import from_delayed
from multipledispatch import dispatch

# from optimus.engines.dask.functions import DaskFunctions as F
from optimus.engines.base.meta import Meta
from optimus.helpers.check import is_dask_dataframe
from optimus.helpers.columns import parse_columns, check_column_numbers, prepare_columns, get_output_cols, \
    validate_columns_names
from optimus.helpers.constants import RELATIVE_ERROR, ProfilerDataTypes, Actions
from optimus.helpers.converter import format_dict
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.functions import collect_as_list, set_function_parser, set_func
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_dict, Infer, profiler_dtype_func, is_list, is_one_element, is_list_of_tuples, is_int, \
    is_tuple, US_STATES_NAMES, regex_full_url
from optimus.profiler.constants import MAX_BUCKETS


class BaseColumns(ABC):
    """Base class for all Cols implementations"""

    def __init__(self, root):
        self.root = root
        self.F = self.root.functions

    def _names(self):
        pass

    @abstractmethod
    def append(self, dfs):
        pass

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
        df = self.root
        columns = parse_columns(df, columns, is_regex=regex, filter_by_column_dtypes=data_type, invert=invert,
                                accepts_missing_cols=accepts_missing_cols)
        meta = df.meta
        dfd = df.data
        if columns is not None:
            dfd = dfd[columns]

        return self.root.new(dfd, meta=meta)

    def copy(self, input_cols, output_cols=None, columns=None):
        """
        Copy one or multiple columns
        :param input_cols: Source column to be copied
        :param output_cols: Destination column
        :param columns: tuple of column [('column1','column_copy')('column1','column1_copy')()]
        :return:
        """
        df = self.root
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

        dfd = df.data
        meta = df.meta

        for input_col, output_col in zip(input_cols, output_cols):
            kw_columns[output_col] = dfd[input_col]
            meta = meta.copy({input_col: output_col})
        dfd = dfd.assign(**kw_columns)

        df = self.root.new(dfd, meta=meta)

        return df.cols.select(output_ordered_columns)

    def drop(self, columns=None, regex=None, data_type=None):
        """
        Drop a list of columns
        :param columns: Columns to be dropped
        :param regex: Regex expression to select the columns
        :param data_type:
        :return:
        """
        df = self.root
        if regex:
            r = re.compile(regex)
            columns = [c for c in list(df.cols.names()) if re.match(r, c)]

        columns = parse_columns(df, columns, filter_by_column_dtypes=data_type)
        check_column_numbers(columns, "*")

        dfd = df.data.drop(columns=columns)
        meta = df.meta.action(Actions.DROP.value, columns)

        return self.root.new(dfd, meta=meta)

    def keep(self, columns=None, regex=None):
        """
        Drop a list of columns
        :param columns: Columns to be dropped
        :param regex: Regex expression to select the columns
        :return:
        """
        df = self.df
        dfd = df.data
        if regex:
            # r = re.compile(regex)
            columns = [c for c in list(df.columns) if re.match(regex, c)]

        columns = parse_columns(df, columns)
        check_column_numbers(columns, "*")

        dfd = dfd.drop(columns=list(set(df.columns) - set(columns)))

        df.meta = Meta.set(df.meta, value=df.meta.preserve(df, Actions.KEEP.value, columns).get())

        return self.root.new(dfd, meta=meta)

    def word_count(self, input_cols, output_cols=None):
        """
        Count words by column element wise
        :param input_cols:
        :param output_cols:
        :return:
        """
        df = self.df
        dfd = df.data

        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)
        for input_col, output_col in zip(input_cols, output_cols):
            dfd[output_col] = dfd[input_col].str.split().str.len()
        return self.root.new(dfd)

    @staticmethod
    @abstractmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        pass

    def _map(self, df, input_col, output_col, func, args, kw_columns):
        pass

    def apply(self, input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
              filter_col_by_dtypes=None, output_cols=None, skip_output_cols_processing=False,
              meta_action=Actions.APPLY_COLS.value, mode="pandas", set_index=False, default=None, **kwargs):

        columns = prepare_columns(self.root, input_cols, output_cols, filter_by_column_dtypes=filter_col_by_dtypes,
                                  accepts_missing_cols=True, default=default)

        kw_columns = {}
        output_ordered_columns = self.names()
        if args is None:
            args = []
        elif not is_tuple(args, ):
            args = (args,)

        df = self.root
        dfd = df.data
        meta = df.meta

        if mode == "whole":
            dfd = func(dfd)
        else:
            for input_col, output_col in columns:
                if mode == "pandas" and (is_dask_dataframe(dfd)):
                    partitions = dfd.to_delayed()
                    delayed_parts = [dask.delayed(func)(part[input_col], *args) for part in partitions]

                    kw_columns[output_col] = from_delayed(delayed_parts)

                elif mode == "vectorized" or mode == "pandas":

                    _ddf = func(dfd[input_col], *args)

                    if is_dask_dataframe(_ddf):
                        df = df.cols.append(_ddf)
                    else:
                        kw_columns[output_col] = func(dfd[input_col], *args)
                elif mode == "map":
                    kw_columns = self._map(dfd, input_col, output_col, func, args, kw_columns)

                # Preserve column order
                if output_col not in self.names():
                    col_index = output_ordered_columns.index(input_col) + 1
                    output_ordered_columns[col_index:col_index] = [output_col]
                from optimus.engines.base.meta import Meta

                # Preserve actions for the profiler
                meta = Meta.action(meta, meta_action, output_col)

            if set_index is True:
                dfd = dfd.reset_index()
            if kw_columns:
                dfd = dfd.assign(**kw_columns)

        # Dataframe to Optimus dataframe
        df = self.root.new(dfd, meta=meta)
        df = df.cols.select(output_ordered_columns)

        return df

    def set(self, where=None, value=None, output_cols=None, default=None):
        """
        Set a column value using a number, string or a expression.
        :param where:
        :param value:
        :param output_cols:
        :param default:
        :return:
        """
        df = self.root
        dfd = df.data

        columns, vfunc = set_function_parser(df, value, where, default)
        # if dfd.cols.dtypes(input_col) == "category":
        #     try:
        #         # Handle error if the category already exist
        #         dfd[input_vcol] = dfd[input_col].cat.add_categories(val_to_list(value))
        #     except ValueError:
        #         pass

        output_cols = one_list_to_val(output_cols)

        if columns:
            final_value = dfd[columns]
        else:
            final_value = dfd
        final_value = final_value.map_partitions(set_func, value=value, where=where, output_col=output_cols,
                                                 parser=vfunc, default=default, meta=object)

        meta = Meta.action(df.meta, Actions.SET.value, output_cols)
        kw_columns = {output_cols: final_value}
        return self.root.new(dfd.assign(**kw_columns), meta=meta)

    @dispatch(object, object)
    def rename(self, columns_old_new=None, func=None):
        """"
        Changes the name of a column(s) dataFrame.
        :param columns_old_new: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
        :param func: can be lower, upper or any string transformation function
        """

        df = self.root
        dfd = df.data
        meta = df.meta

        # Apply a transformation function
        if is_list_of_tuples(columns_old_new):
            validate_columns_names(df, columns_old_new)
            for col_name in columns_old_new:

                old_col_name = col_name[0]
                if is_int(old_col_name):
                    old_col_name = df.cols.names()[old_col_name]
                if func:
                    old_col_name = func(old_col_name)

                # DaskColumns.set_meta(col_name, "optimus.transformations", "rename", append=True)
                # TODO: this seems to the only change in this function compare to pandas. Maybe this can
                #  be moved to a base class

                new_col_name = col_name[1]
                if old_col_name != col_name:
                    dfd = dfd.rename(columns={old_col_name: new_col_name})
                    meta = Meta.rename(meta, {old_col_name: new_col_name})

        df = self.root.new(dfd, meta=meta)
        df.meta = meta

        return self.root.new(df.data, meta=meta)

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
        df = self.root
        columns = parse_columns(df, columns)
        result = {}

        for col_name in columns:
            # column_meta = glom(df.meta.get(), f"profile.columns.{col_name}", skip_exc=KeyError)
            column_meta = df.meta.get(f"profile.columns.{col_name}")
            if column_meta is None:
                result[col_name] = None
            else:
                result[col_name] = column_meta["profiler_dtype"]
        return result

    def set_profiler_dtypes(self, columns: dict):
        """
        Set profiler data type
        :param columns: A dict with the form {"col_name": profiler datatype}
        :return:
        """
        df = self.root
        meta = df.meta

        for col_name, props in columns.items():
            dtype = props["dtype"]
            if dtype in ProfilerDataTypes.list():
                Meta.set(meta, f"profile.columns.{col_name}.profiler_dtype", props)
                Meta.action(meta, Actions.PROFILER_DTYPE.value, col_name)
            else:
                RaiseIt.value_error(dtype, ProfilerDataTypes.list())

        return df

    def cast(self, input_cols=None, dtype=None, output_cols=None, columns=None):
        """
        NOTE: We have two ways to cast the data. Use the use the native .astype() this is faster but can not handle some
        trnasformation like string to number in which should output nan.

        is pendulum faster than pd.to_datatime
        We could use astype str and boolean


        Cast the elements inside a column or a list of columns to a specific data type.
        Unlike 'cast' this not change the columns data type

        :param input_cols: Columns names to be casted
        :param output_cols:
        :param dtype: final data type
        :param columns: List of tuples of column names and types to be casted. This variable should have the
                following structure:
                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]
                The first parameter in each tuple is the column name, the second is the final datatype of column after
                the transformation is made.
        """

        df = self.root

        columns = prepare_columns(df, input_cols, output_cols, args=dtype)
        for input_col, output_col, arg in columns:

            if arg == "float":
                df = df.cols.to_float(input_col, output_col)
            elif arg == "int":
                df = df.cols.to_integer(input_col, output_col)
            elif arg == "datetime":
                df = df.cols.to_datetime(input_col, output_col)
            elif arg == "bool":
                df = df.cols.to_boolean(input_col, output_col)
            elif arg == "str":
                df = df.cols.to_string(input_col, output_col)
            else:
                RaiseIt.value_error(arg, ["float", "integer", "datetime", "bool", "str"])

        return df

    @staticmethod
    @abstractmethod
    def astype(*args, **kwargs):
        pass

    def pattern(self, input_cols="*", output_cols=None, mode=0):
        """
        Replace alphanumeric and punctuation chars for canned chars. We aim to help to find string patterns
        c = Any alpha char in lower or upper case
        l = Any alpha char in lower case
        U = Any alpha char in upper case
        * = Any alphanumeric in lower or upper case. Used only in type 2 nd 3
        # = Any numeric
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

        df = self.root
        columns = prepare_columns(df, input_cols, output_cols)

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
        else:
            RaiseIt.value_error(mode, ["0", "1", "2", "3"])

        kw_columns = {}

        for input_col, output_col in columns:
            kw_columns[output_col] = df.cols.select(input_col).cols.to_string().cols.remove_accents().cols.replace(
                search=search_by, replace_by=replace_by).data[input_col]

        return df.cols.assign(kw_columns)

    def assign(self, kw_columns):
        dfd = self.root.data
        return self.root.new(dfd.assign(**kw_columns))

    # TODO: Consider implement lru_cache for caching
    def pattern_counts(self, input_cols, n=10, mode=0, flush=False):
        """
        Count how many equal patters there are in a columns. Handle cache to trigger the operation on if necessary
        :param input_cols:
        :param n: top n number
        :param mode:
        :param flush: FLush cache to reprocess
        :return:
        """

        df = self.root

        result = {}
        input_cols = parse_columns(df, input_cols)
        for input_col in input_cols:
            column_modified_time = df.meta.get(f"profile.columns.{input_col}.modified")
            patterns_update_time = df.meta.get(f"profile.columns.{input_col}.patterns.updated")
            if column_modified_time is None:
                column_modified_time = -1
            if patterns_update_time is None:
                patterns_update_time = 0

            _patterns_values = df.meta.get(f"profile.columns.{input_col}.patterns.values")
            if _patterns_values is not None:
                cached = len(_patterns_values)

            if column_modified_time > patterns_update_time \
                    or patterns_update_time == 0 \
                    or flush is True \
                    or cached != n:

                # Plus n + 1 so we can could let the user know if there are more patterns
                result[input_col] = \
                    df.cols.pattern(input_col, mode=mode).cols.frequency(input_col, n=n + 1)["frequency"][
                        input_col]

                if len(result[input_col]["values"]) > n:
                    result[input_col].update({"more": True})

                    # Remove extra element from list
                    result[input_col]["values"].pop()

                df.meta = Meta.set(df.meta, f"profile.columns.{input_col}.patterns", result[input_col])
                df.meta = Meta.set(df.meta, f"profile.columns.{input_col}.patterns.updated", time.time())


            else:
                result[input_col] = df.meta.get(f"profile.columns.{input_col}.patterns")

        return result

    def groupby(self, by, agg, order="asc", *args, **kwargs):
        """
        This helper function aims to help managing columns name in the aggregation output.
        Also how to handle ordering columns because dask can order columns
        :param by: Column names
        :param agg:
        :param order:
        :param args:
        :param kwargs:
        :return:
        """
        df = self.root.data
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

        df_left = self.root.data

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

        left_names = df_left.cols.names()
        right_names = df_right.cols.names()

        last_column_name = left_names[-1]
        # Use to reorder de output
        df_left = df_left.merge(df_right, how=how, on=on, left_on=left_on, right_on=right_on,
                                suffixes=(suffix_left, suffix_right))

        # Remove duplicated index if the name is the same. If the index name are not the same
        if order is True:
            names = df_left.cols.names()
            last_column_name = last_column_name if last_column_name in names else last_column_name + suffix_left
            left_on = left_on if left_on in names else left_on + suffix_left
            right_on = right_on if right_on in names else right_on + suffix_right
            if left_on in names:
                df_left = df_left.cols.move(left_on, "before", last_column_name)
            if right_on in names:
                df_left = df_left.cols.move(right_on, "before", last_column_name)

        return df_left

    def is_match(self, columns, dtype, invert=False):
        """
        Find the rows that match a data type
        :param columns:
        :param dtype: data type to match
        :param invert: Invert the match
        :return:
        """
        df = self.root
        dfd = df.data
        columns = parse_columns(df, columns)

        # dtype = parse_dtypes(dfd, dtype)
        f = profiler_dtype_func(dtype)
        if f is not None:
            for col_name in columns:
                dfd = dfd[col_name].apply(f)
                dfd = ~dfd if invert is True else dfd
        return self.root.new(dfd)

    def move(self, column, position, ref_col=None):
        """
        Move a column to specific position
        :param column: Column to be moved
        :param position: Column new position. Accepts 'after', 'before', 'beginning', 'end'
        :param ref_col: Column taken as reference
        :return: Spark DataFrame
        """
        df = self.root
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
        df = self.root
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
        :return: {col_name: dtype}
        """
        df = self.root
        columns = parse_columns(df, columns)
        data_types = ({k: str(v) for k, v in dict(df.data.dtypes).items()})
        return {col_name: data_types[col_name] for col_name in columns}

    def schema_dtype(self, columns="*"):
        """
        Return the column(s) data type as Type
        :param columns: Columns to be processed
        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)
        dfd = df.data
        result = {}
        for col_name in columns:
            if dfd[col_name].dtype.name == "category":
                result[col_name] = "category"
            else:
                result[col_name] = np.dtype(dfd[col_name]).type
        return format_dict(result)

    def agg_exprs(self, columns, funcs, *args, compute=True, tidy=True):
        """
        Create and run aggregation
        :param columns:
        :param funcs:
        :param args:
        :param compute:
        :param tidy:
        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)

        if args is None:
            args = []
        elif not is_tuple(args, ):
            args = (args,)

        funcs = val_to_list(funcs)
        all_funcs = [{func.__name__: {col_name: func(df.data[col_name], *args)}} for col_name in columns for
                     func in
                     funcs]
        a = self.exec_agg(all_funcs, compute)
        result = {}

        # Reformat aggregation
        for i in a:
            for x, y in i.items():
                result.setdefault(x, {}).update(y)

        return format_dict(result, tidy)

    @staticmethod
    @abstractmethod
    def exec_agg(exprs, compute):
        pass

    def mad(self, columns="*", relative_error=RELATIVE_ERROR, more=False, tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.mad, relative_error, more, compute=compute, tidy=tidy)

    def min(self, columns="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.min, compute=compute, tidy=tidy)

    def max(self, columns="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.max, compute=compute, tidy=tidy)

    def mode(self, columns="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.mode, compute=compute, tidy=tidy)

    def range(self, columns="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.range, compute=compute, tidy=tidy)

    def percentile(self, columns="*", values=None, relative_error=RELATIVE_ERROR, tidy=True, compute=True):
        df = self.root

        if values is None:
            values = [0.25, 0.5, 0.75]
        return df.cols.agg_exprs(columns, self.F.percentile, values, relative_error, tidy=tidy, compute=True)

    def median(self, columns="*", relative_error=RELATIVE_ERROR, tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.percentile, [0.5], relative_error, tidy=tidy, compute=True)

    # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
    def kurtosis(self, columns="*", tidy=True, compute=False):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.kurtosis, tidy=tidy, compute=compute)

    def skew(self, columns="*", tidy=True, compute=False):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.skew, tidy=tidy, compute=compute)

    def mean(self, columns="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.mean, tidy=tidy, compute=compute)

    def sum(self, columns="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.sum, tidy=tidy, compute=compute)

    def var(self, columns="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.var, tidy=tidy, compute=compute)

    def std(self, columns="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.std, tidy=tidy, compute=compute)

    # Math Operations
    def abs(self, input_cols="*", output_cols=None):
        """
        Apply abs to column
        :param input_cols:
        :param output_cols:
        :return:
        """

        return self.apply(input_cols, self.F.abs, output_cols=output_cols, meta_action=Actions.ABS.value,
                          mode="vectorized")

    def exp(self, input_cols="*", output_cols=None):
        """
        Returns Euler's number, e (~2.718) raised to a power.
        :param input_cols:
        :param output_cols:
        :return:
        """

        return self.apply(input_cols, self.F.exp, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized")

    def mod(self, input_cols="*", divisor=2, output_cols=None):
        """
        Apply mod to column
        :param input_cols:
        :param divisor:
        :param output_cols:
        :return:(
        """

        return self.apply(input_cols, self.F.mod, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized", args=divisor)

    def log(self, input_cols="*", output_cols=None):
        """
        Apply mod to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        return self.apply(input_cols, self.F.log, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized")

    def ln(self, input_cols="*", output_cols=None):
        """
        Apply mod to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        return self.apply(input_cols, self.F.ln, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized")

    def pow(self, input_cols="*", other=2, output_cols=None):
        """
        Apply mod to column
        :param other:
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.pow, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized", args=other)

    def sqrt(self, input_cols="*", output_cols=None):
        """
        Apply sqrt to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        return self.apply(input_cols, self.F.sqrt, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized")

    def round(self, input_cols="*", decimals=1, output_cols=None):
        """

        :param input_cols:
        :param decimals:
        :param output_cols:
        :return:
        """
        df = self.root
        return df.cols.apply(input_cols, self.F.round, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized", args=decimals)

    def floor(self, input_cols="*", output_cols=None):
        """

        :param input_cols:
        :param output_cols:
        :return:
        """
        df = self.root
        return df.cols.apply(input_cols, self.F.floor, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def ceil(self, input_cols="*", output_cols=None):
        """
        Apply ceil to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.ceil, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    # Trigonometric
    def sin(self, input_cols="*", output_cols=None):
        """
        Apply sin to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.sin, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def cos(self, input_cols="*", output_cols=None):
        """
        Apply cos to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.cos, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def tan(self, input_cols="*", output_cols=None):
        """
        Apply cos to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.tan, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def asin(self, input_cols="*", output_cols=None):
        """
        Apply cos to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.asin, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def acos(self, input_cols="*", output_cols=None):
        """
        Apply cos to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.acos, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def atan(self, input_cols="*", output_cols=None):
        """
        Apply cos to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.atan, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def sinh(self, input_cols="*", output_cols=None):
        """
        Apply sin to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.sinh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def cosh(self, input_cols="*", output_cols=None):
        """
        Apply cos to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.cosh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def tanh(self, input_cols="*", output_cols=None):
        """
        Apply cos to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.tanh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def asinh(self, input_cols="*", output_cols=None):
        """
        Apply sin to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.asinh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def acosh(self, input_cols="*", output_cols=None):
        """
        Apply cos to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.acosh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def atanh(self, input_cols="*", output_cols=None):
        """
        Apply cos to column
        :param input_cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(input_cols, self.F.atanh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def extract(self, input_cols, regex, output_cols=None):
        def _extract(_value, _regex):
            return self.F.extract(_value, _regex)

        return self.apply(input_cols, _extract, args=(regex,), func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.EXTRACT.value, mode="vectorized")

        # def replace_regex(input_cols, regex=None, value=None, output_cols=None):

    def slice(self, input_cols, start, stop, step, output_cols=None):
        def _slice(value, _start, _stop, _step):
            return self.F.slice(value, _start, _stop, _step)

        return self.apply(input_cols, _slice, args=(start, stop, step), func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.SLICE.value, mode="vectorized")

    def left(self, input_cols, n, output_cols=None):

        df = self.apply(input_cols, self.F.left, args=(n,), func_return_type=str,
                        output_cols=output_cols, meta_action=Actions.LEFT.value, mode="vectorized")
        return df

    def right(self, input_cols, n, output_cols=None):
        df = self.apply(input_cols, self.F.right, args=(n,), func_return_type=str,
                        output_cols=output_cols, meta_action=Actions.RIGHT.value, mode="vectorized")
        return df

    def mid(self, input_cols, start=0, n=1, output_cols=None):

        df = self.apply(input_cols, self.F.mid, args=(start, n), func_return_type=str,
                        output_cols=output_cols, meta_action=Actions.MID.value, mode="vectorized")
        return df

    def to_float(self, input_cols="*", output_cols=None):
        return self.apply(input_cols, self.F.to_float, func_return_type=float,
                          output_cols=output_cols, meta_action=Actions.TO_FLOAT.value, mode="map")

    def to_integer(self, input_cols="*", output_cols=None):
        # Filter columns that are not integer
        # filtered_columns = []
        # df = self.parent
        #
        # input_cols = parse_columns(df, input_cols)
        # for col_name in input_cols:
        #     if df.data[col_name].dtype != np.int64:
        #         filtered_columns.append(col_name)
        #
        # print("filtered_columns", filtered_columns)
        # if len(filtered_columns) > 0:
        return self.apply(input_cols, self.F.to_integer, func_return_type=int,
                          output_cols=output_cols, meta_action=Actions.TO_INTEGER.value, mode="map")
        # else:
        #     return df

    def to_string(self, input_cols="*", output_cols=None):
        filtered_columns = []
        df = self.root

        input_cols = parse_columns(df, input_cols)
        for col_name in input_cols:
            dtype = df.data[col_name].dtype

            if dtype != np.object:
                filtered_columns.append(col_name)

        if len(filtered_columns) > 0:
            return self.apply(input_cols, self.F.to_string, func_return_type=str,
                              output_cols=output_cols, meta_action=Actions.LOWER.value, mode="vectorized",
                              func_type="column_expr")
        else:
            return df

    def lower(self, input_cols="*", output_cols=None):
        return self.apply(input_cols, self.F.lower, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.LOWER.value, mode="vectorized", func_type="column_expr")

    def upper(self, input_cols="*", output_cols=None):
        return self.apply(input_cols, self.F.upper, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.UPPER.value, mode="vectorized", func_type="column_expr")

    def title(self, input_cols="*", output_cols=None):
        return self.apply(input_cols, self.F.title, func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.PROPER.value, mode="vectorized",
                          func_type="column_expr")

    def capitalize(self, input_cols="*", output_cols=None):
        return self.apply(input_cols, self.F.capitalize, func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.PROPER.value, mode="vectorized",
                          func_type="column_expr")

    def proper(self, input_cols="*", output_cols=None):
        return self.apply(input_cols, self.F.proper, func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.PROPER.value, mode="vectorized",
                          func_type="column_expr")

    # def url_decode(self):
    #     from urllib.parse import unquote
    #     def title_parse(title):
    #         title = unquote(title)
    #         return title
    #
    #     # "apply" from pandas method will help to all the decode text in the csv
    #     df['title'] = df.title.apply(title_parse)

    def pad(self, input_cols="*", width=0, side="left", fillchar="0", output_cols=None, ):

        return self.apply(input_cols, self.F.pad, args=(width, side, fillchar,), func_return_type=str,
                          output_cols=output_cols,
                          meta_action=Actions.PAD.value, mode="vectorized")

    def trim(self, input_cols="*", output_cols=None):
        return self.apply(input_cols, self.F.trim, func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.TRIM.value, mode="vectorized")

    def date_format(self, input_cols, current_format=None, output_format=None, output_cols=None):

        def _date_format(value, *args):
            _current_format, _output_format = args
            return self.F.date_format(value, _current_format, _output_format)

        df = self.root
        return df.cols.apply(input_cols, _date_format, args=(current_format, output_format), func_return_type=str,
                             output_cols=output_cols, meta_action=Actions.DATE_FORMAT.value, mode="pandas",
                             set_index=True)

    @staticmethod
    @abstractmethod
    def reverse(input_cols, output_cols=None):
        pass

    def remove(self, input_cols, search=None, search_by="chars", output_cols=None):
        return self.replace(input_cols=input_cols, search=search, replace_by="", search_by=search_by,
                            output_cols=output_cols)

    def remove_accents(self, input_cols="*", output_cols=None):
        """
        Remove diacritics from a dataframe
        :param input_cols:
        :param output_cols:
        :return:
        """

        return self.apply(input_cols, self.F.remove_accents, func_return_type=str,
                          meta_action=Actions.REMOVE_ACCENTS.value,
                          output_cols=output_cols, mode="vectorized")

    def remove_numbers(self, input_cols, output_cols=None):
        """
        Remove numbers from a dataframe
        :param input_cols:
        :param output_cols:
        :return:
        """

        def _remove_numbers(value):
            return value.astype(str).str.replace(r'\d+', '')

        return self.apply(input_cols, _remove_numbers, func_return_type=str,
                          output_cols=output_cols, mode="pandas", set_index=True)

    def remove_white_spaces(self, input_cols="*", output_cols=None):
        """
        Remove all white spaces from a dataframe
        :param input_cols:
        :param output_cols:
        :return:
        """

        return self.apply(input_cols, self.F.remove_white_spaces, func_return_type=str,
                          output_cols=output_cols, mode="vectorized")

    def remove_special_chars(self, input_cols="*", output_cols=None):
        """
        Remove special chars from a dataframe
        :param input_cols:
        :param output_cols:
        :return:
        """

        return self.apply(input_cols, self.F.remove_special_chars, func_return_type=str,
                          output_cols=output_cols, mode="vectorized")

    def to_datetime(self, input_cols, format, output_cols=None):
        """

        :param input_cols:
        :param format:
        :param output_cols:
        :return:
        """
        return self.apply(input_cols, self.F().to_datetime, func_return_type=str,
                          output_cols=output_cols, args=format, mode="pandas")

    def year(self, input_cols, format=None, output_cols=None):
        """

        :param input_cols:
        :param format:
        :param output_cols:
        :return:
        """

        return self.apply(input_cols, self.F().year, args=format, output_cols=output_cols,
                          meta_action=Actions.YEAR.value,
                          mode="pandas", set_index=True)

    def month(self, input_cols, format=None, output_cols=None):
        """

        :param input_cols:
        :param format:
        :param output_cols:
        :return:
        """

        return self.apply(input_cols, self.F.month(), args=format, output_cols=output_cols, mode="pandas",
                          set_index=True)

    def day(self, input_cols, format=None, output_cols=None):

        return self.apply(input_cols, self.F.day, args=format, output_cols=output_cols, mode="pandas", set_index=True)

    def hour(self, input_cols, format=None, output_cols=None):

        def _hour(value, _format):
            return self.F.hour(value, _format)

        return self.apply(input_cols, _hour, args=format, output_cols=output_cols, mode="pandas", set_index=True)

    def minute(self, input_cols, format=None, output_cols=None):

        def _minute(value, _format):
            return self.F.minute(value, _format)

        return self.apply(input_cols, _minute, args=format, output_cols=output_cols, mode="pandas", set_index=True)

    def second(self, input_cols, format=None, output_cols=None):

        def _second(value, _format):
            return self.F.second(value, _format)

        return self.apply(input_cols, _second, args=format, output_cols=output_cols, mode="pandas", set_index=True)

    def weekday(self, input_cols, format=None, output_cols=None):

        def _second(value, _format):
            return self.F.weekday(value, _format)

        return self.apply(input_cols, _second, args=format, output_cols=output_cols, mode="pandas", set_index=True)

    def years_between(self, input_cols, date_format=None, output_cols=None):

        def _years_between(value, args):
            return self.F.years_between(value, *args)

        return self.apply(input_cols, _years_between, args=[date_format], func_return_type=str,
                          output_cols=output_cols,
                          meta_action=Actions.YEARS_BETWEEN.value, mode="pandas", set_index=True)

    def replace(self, input_cols="*", search=None, replace_by=None, search_by="chars", ignore_case=False,
                output_cols=None):
        """
        Replace a value, list of values by a specified string
        :param input_cols: '*', list of columns names or a single column name.
        :param search: Values to look at to be replaced
        :param replace_by: New value to replace the old one
        :param search_by: Can be "full","words","chars" or "numeric".
        :param ignore_case: Ignore case when searching for match
        :param output_cols:
        :return: DataFrame
        """

        # df = self.parent

        if search_by == "chars":
            # print("F", type(F), F)
            func = self.F.replace_chars
        elif search_by == "words":
            func = self.F.replace_words
        elif search_by == "full":
            func = self.F.replace_full
        else:
            RaiseIt.value_error(search_by, ["chars", "words", "full"])

        # Cudf raise and exception if both param are not the same type
        # For example [] ValueError: Cannot convert value of type list  to cudf scalar
        search = val_to_list(search)
        replace_by = val_to_list(replace_by)
        return self.apply(input_cols, func, args=(search, replace_by), func_return_type=str,
                          output_cols=output_cols,
                          meta_action=Actions.REPLACE.value, mode="vectorized")

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
        df = self.root

        def _fill_na(series, *args):
            _value = args[0]
            return series.fillna(_value)

        return df.cols.apply(input_cols, _fill_na, args=value, output_cols=output_cols, mode="vectorized")

    def is_na(self, input_cols, output_cols=None):
        """
        Replace null values with True and non null with False
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _is_na(value):
            return value.isnull()

        df = self.root
        return df.cols.apply(input_cols, _is_na, output_cols=output_cols, mode="vectorized")

    def count(self):
        df = self.root
        return len(df.cols.names())

    def count_na(self, columns="*", tidy=True, compute=True):
        """
        Return the NAN and Null count in a Column
        :param columns: '*', list of columns names or a single column name.
        :param tidy:
        :param compute:
        :return:
        """
        df = self.root
        return df.cols.agg_exprs(columns, self.F.count_na, tidy=tidy, compute=compute)

    def unique(self, columns, values=None, relative_error=RELATIVE_ERROR, tidy=True, compute=True):
        df = self.root

        return df.cols.agg_exprs(columns, self.F.unique, tidy=tidy, compute=compute)

    def count_uniques(self, columns, values=None, estimate=True, tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.count_uniques, values, estimate, tidy=tidy, compute=compute)

    def _math(self, columns, operator, output_col):

        """
        Helper to process arithmetic operation between columns. If a
        :param columns: Columns to be used to make the calculation
        :param operator: A lambda function
        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)
        expr = reduce(operator, [df[col_name].to_float().fillna(0) for col_name in columns])
        return df.assign(**{output_col: expr})

    def add(self, columns, output_col="sum"):
        """
        Add two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_col:
        :return:
        """
        df = self.root
        return df.cols._math(columns, lambda x, y: x + y, output_col)

    def sub(self, columns, output_col="sub"):
        """
        Subs two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_col:
        :return:
        """
        df = self.root
        return df.cols._math(columns, lambda x, y: x - y, output_col)

    def mul(self, columns, output_col="mul"):
        """
        Multiply two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_col:
        :return:
        """
        df = self.root
        return df.cols._math(columns, lambda x, y: x * y, output_col)

    def div(self, columns, output_col="div"):
        """
        Divide two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_col:
        :return:
        """
        df = self.root
        return df.cols._math(columns, lambda x, y: x / y, output_col)

    def z_score(self, input_cols, output_cols=None):

        df = self.root

        def _z_score(value):
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
        df = self.root
        iqr_result = {}
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        quartile = df.cols.percentile(columns, [0.25, 0.5, 0.75], relative_error=relative_error, tidy=False)[
            "percentile"]
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
        :param output_cols: Resulted on or multiple columns after the unnest operation [(output_col_1_1,output_col_1_2),
        (output_col_2_1, output_col_2]
        :param separator: char or regex
        :param splits: Number of columns splits.
        :param index: Return a specific index per columns. [1,2]
        :param drop:
        :param mode:
        """
        df = self.root

        if separator is not None:
            separator = re.escape(separator)

        input_cols = parse_columns(df, input_cols)

        index = val_to_list(index)
        output_ordered_columns = df.cols.names()

        dfd = df.data

        for idx, input_col in enumerate(input_cols):

            if is_list_of_tuples(index):
                final_index = index[idx]
            else:
                final_index = index

            if output_cols is None:
                final_columns = [input_col + "_" + str(i) for i in range(splits)]
            elif is_list_of_tuples(output_cols):
                final_columns = output_cols[idx]
            else:
                final_columns = [output_cols + "_" + str(i) for i in range(splits)]

            if mode == "string":
                dfd_new = dfd[input_col].astype(str).str.split(separator, expand=True, n=splits - 1)

            elif mode == "array":
                if is_dask_dataframe(dfd):
                    def func(value):
                        pdf = value.apply(pd.Series)
                        pdf.columns = final_columns
                        return pdf

                    dfd_new = dfd[input_col].map_partitions(func, meta={c: object for c in final_columns})
                else:
                    dfd_new = dfd[input_col].apply(pd.Series)

            # If columns split is shorter than the number of splits
            dfd_new.columns = final_columns[:len(dfd_new.columns)]
            df_new = df.new(dfd_new)
            if final_index:
                df_new = df_new.cols.select(final_index[idx])
            df = df.cols.append(df_new)

        if drop is True:
            df = df.drop(columns=input_cols)
            for input_col in input_cols:
                if input_col in output_ordered_columns:
                    output_ordered_columns.remove(input_col)

        meta = df.meta
        meta = meta.preserve(df, Actions.UNNEST.value, final_columns)

        df = df.cols.move(df_new.cols.names(), "after", input_cols)

        return df

    @staticmethod
    @abstractmethod
    def scatter(columns, buckets=10):
        pass

    def hist(self, columns="*", buckets=20, compute=True):

        df = self.root
        columns = parse_columns(df, columns)

        @df.delayed
        def _bins_col(_columns, _min, _max):
            return {col_name: list(np.linspace(_min["min"][col_name], _max["max"][col_name], num=buckets)) for
                    col_name in _columns}

        _min = df.cols.min(columns, compute=False, tidy=False)
        _max = df.cols.max(columns, compute=False, tidy=False)
        _bins = _bins_col(columns, _min, _max)

        @df.delayed
        def _hist(pdf, col_name, _bins):
            # import cupy as cp
            _count, bins_edges = np.histogram(pd.to_numeric(pdf, errors='coerce'), bins=_bins[col_name])
            # _count, bins_edges = np.histogram(self.to_float(col_name).data[col_name], bins=_bins[col_name])
            # _count, bins_edges = cp.histogram(cp.array(_series.to_gpu_array()), buckets)
            return {col_name: [list(_count), list(bins_edges)]}

        @df.delayed
        def _agg_hist(values):
            _result = {}
            x = np.zeros(buckets - 1)
            for i in values:
                for j in i:
                    t = i.get(j)
                    if t is not None:
                        _count = np.sum([x, t[0]], axis=0)
                        _bins = t[1]
                        col_name = j
                l = len(_count)
                r = [{"lower": float(_bins[i]), "upper": float(_bins[i + 1]),
                      "count": int(_count[i])} for i in range(l)]
                _result[col_name] = r

            return {"hist": _result}

        # print("df.data",type(df.data),df.data)
        partitions = df.to_delayed()
        c = [_hist(part[col_name], col_name, _bins) for part in partitions for col_name in columns]

        d = _agg_hist(c)

        if is_dict(d):
            result = d
        elif compute:
            result = d.compute()
        return result

    def count_mismatch(self, columns_type: dict = None, compute=True):
        """
        Count mismatches values in columns
        :param columns_type:
        :return: {'col_name': {'mismatch': 0, 'missing': 9, 'match': 0, 'profiler_dtype': 'object'}}
        """
        df = self.root
        dfd = df.data

        result = {}
        nulls = df.cols.count_na(tidy=False)["count_na"]
        total_rows = df.rows.count()
        # TODO: Test this cudf.Series(cudf.core.column.string.cpp_is_integer(a["A"]._column)) and fast_numbers

        for col_name, props in columns_type.items():
            dtype = props["dtype"]

            result[col_name] = {"match": 0, "missing": 0, "mismatch": 0}
            result[col_name]["missing"] = int(nulls.get(col_name))

            if dtype == ProfilerDataTypes.STRING.value:
                match = total_rows - nulls.get(col_name)
                mismatch = nulls[col_name]

            elif dtype == ProfilerDataTypes.US_STATE.value:
                match = dfd[col_name].astype(str).str.isin(US_STATES_NAMES).value_counts().to_dict()
                mismatch = total_rows - match
            else:
                match = df.cols.select(col_name).cols.to_string().cols.match(col_name,
                                                                             Infer.ProfilerDataTypesFunctions[
                                                                                 dtype]).cols.frequency()

                v = {list(j.values())[0]: list(j.values())[1] for j in match["frequency"][col_name]["values"]}

                match = v.get("True")
                mismatch = v.get("False")

            result[col_name]["match"] = 0 if match is None else int(match)
            result[col_name]["mismatch"] = 0 if mismatch is None else int(mismatch)

        for col_name in columns_type.keys():
            result[col_name].update({"profiler_dtype": columns_type[col_name]})
        return result

    @staticmethod
    @abstractmethod
    def count_by_dtypes(columns, infer=False, str_funcs=None, int_funcs=None):
        pass

    def quality(self, columns="*"):
        """
        Infer the datatype and return the match. mismatch and profiler datatype  for every column.
        In case of date it returns also the format datatype
        :param columns:
        :return:
        """
        df = self.root
        a = df.cols.infer_profiler_dtypes(columns)
        return df.cols.count_mismatch(a)

    def infer_profiler_dtypes(self, columns="*"):
        """
        Infer datatypes in a dataframe from a sample
        :param columns:
        :return:Return a dict with the column and the inferred data type
        """
        df = self.root

        columns = parse_columns(df, columns)
        total_preview_rows = 30

        # Infer the data type from every element in a Series.
        # FIX: could this be vectorized?
        sample = df.cols.select(columns).rows.limit(total_preview_rows).to_pandas()
        pdf = sample.applymap(Infer.parse_pandas)

        cols_and_inferred_dtype = {}
        for col_name in columns:
            _value_counts = pdf[col_name].value_counts()
            dtype = _value_counts.index[0]

            if dtype != "null" and dtype != ProfilerDataTypes.MISSING.value:
                r = dtype
            elif _value_counts[0] < len(pdf):
                r = _value_counts.index[1]
            else:
                r = ProfilerDataTypes.OBJECT.value

            cols_and_inferred_dtype[col_name] = {"dtype": r}
            if dtype == "date":
                # pydatainfer do not accepts None value so we must filter tham
                filtered_dates = [i for i in sample[col_name].to_list() if i]
                cols_and_inferred_dtype[col_name].update({"format": pydateinfer.infer(filtered_dates)})
        return cols_and_inferred_dtype

    def match(self, input_cols, regex):
        dfd = self.root.data

        return self.root.new(dfd[input_cols].str.match(regex).to_frame())

    # def _series_to_dict(series):
    #     return series.to_dict()

    def frequency(self, columns="*", n=MAX_BUCKETS, percentage=False, total_rows=None, count_uniques=False,
                  compute=True, tidy=False):

        df = self.root
        columns = parse_columns(df, columns)

        @df.delayed
        def series_to_dict(_series, _total_freq_count=None):
            _result = [{"value": i, "count": j} for i, j in _series.to_dict().items()]

            if _total_freq_count is None:
                _result = {_series.name: {"values": _result}}
            else:
                _result = {_series.name: {"values": _result, "count_uniques": int(_total_freq_count)}}

            return _result

        @df.delayed
        def flat_dict(top_n):
            return {"frequency": {key: value for ele in top_n for key, value in ele.items()}}

        @df.delayed
        def freq_percentage(_value_counts, _total_rows):

            for i, j in _value_counts.items():
                for x in list(j.values())[0]:
                    x["percentage"] = round((x["count"] * 100 / _total_rows), 2)

            return _value_counts

        value_counts = [df.cols.to_string().data[col_name].value_counts() for col_name in columns]

        n_largest = [_value_counts.nlargest(n) for _value_counts in value_counts]

        if count_uniques is True:
            count_uniques = [_value_counts.count() for _value_counts in value_counts]
            b = [series_to_dict(_n_largest, _count) for _n_largest, _count in zip(n_largest, count_uniques)]
        else:
            b = [series_to_dict(_n_largest) for _n_largest in n_largest]

        c = flat_dict(b)

        if percentage:
            c = freq_percentage(c, df.delayed(len)(df))

        if compute is True:
            result = dd.compute(c)[0]
        else:
            result = c

        # if tidy is True:
        #     format_dict(result)
        return result

    @staticmethod
    @abstractmethod
    def correlation(input_cols, method="pearson", output="json"):
        pass

    def boxplot(self, columns):
        """
        Output values frequency in json format
        :param columns: Columns to be processed
        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)

        for col_name in columns:
            iqr = df.cols.iqr(col_name, more=True)
            lb = iqr["q1"] - (iqr["iqr"] * 1.5)
            ub = iqr["q3"] + (iqr["iqr"] * 1.5)

            _mean = df.cols.mean(columns)

            query = ((df(col_name) < lb) | (df(col_name) > ub))
            fliers = collect_as_list(df.rows.select(query).cols.select(col_name).limit(1000))
            stats = [{'mean': _mean, 'med': iqr["q2"], 'q1': iqr["q1"], 'q3': iqr["q3"], 'whislo': lb, 'whishi': ub,
                      'fliers': fliers, 'label': one_list_to_val(col_name)}]

            return stats
        pass

    def names(self, col_names="*", by_dtypes=None, invert=False):

        columns = parse_columns(self.root, col_names, filter_by_column_dtypes=by_dtypes, invert=invert)
        return columns

    def count_zeros(self, columns, tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(columns, self.F.count_zeros, tidy=True, compute=True)

    @staticmethod
    @abstractmethod
    def qcut(columns, num_buckets, handle_invalid="skip"):
        pass

    def cut(self, input_cols, bins, output_cols=None):
        df = self.root

        def _cut(value, args):
            return self.F.cut(value, bins)

        return self.apply(input_cols, _cut, output_cols=output_cols, meta_action=Actions.CUT.value,
                          mode="vectorized")

    def clip(self, input_cols, lower_bound, upper_bound, output_cols=None):
        df = self.root

        def _clip(value):
            return self.F.clip(value, lower_bound, upper_bound)

        return df.cols.apply(input_cols, _clip, output_cols=output_cols, meta_action=Actions.CLIP.value,
                             mode="vectorized")

    @staticmethod
    @abstractmethod
    def string_to_index(input_cols=None, output_cols=None, columns=None):
        pass

    @staticmethod
    @abstractmethod
    def index_to_string(input_cols=None, output_cols=None, columns=None):
        pass

    # URL methods
    def domain(self, input_cols, output_cols=None):
        """
        From https://www.hi-bumblebee.com it returns hi-bumblebee.com
        :param input_cols:
        :param output_cols:
        :return:
        """

        def _domain(value):
            return value.str.extract(regex_full_url)[6]

        df = self.root
        return df.cols.apply(input_cols, _domain, output_cols=output_cols, meta_action=Actions.DOMAIN.value,
                             mode="vectorized")

    def domain_scheme(self, input_cols, output_cols=None):
        # From https://www.hi-bumblebee.com it returns https
        def _domain_scheme(value):
            return value.str.extract(regex_full_url)[2]

        df = self.root
        return df.cols.apply(input_cols, _domain_scheme, output_cols=output_cols,
                             meta_action=Actions.DOMAIN_SCHEME.value,
                             mode="vectorized")

    def domain_params(self, input_cols, output_cols=None):
        def _domain_params(value):
            return value.str.extract(regex_full_url)[9]

        df = self.root
        return df.cols.apply(input_cols, _domain_params, output_cols=output_cols,
                             meta_action=Actions.DOMAIN_PARAMS.value,
                             mode="vectorized")

    def domain_path(self, input_cols, output_cols=None):

        def _domain_path(value):
            return value.str.extract(regex_full_url)[7]

        df = self.root
        return df.cols.apply(input_cols, _domain_path, output_cols=output_cols, meta_action=Actions.DOMAIN_PATH.value,
                             mode="vectorized")

    def port(self, input_cols, output_cols=None):
        # From https://www.hi-bumblebee.com:8080 it returns 8080
        def _port(value):
            return value.str.extract(regex_full_url)[7]

        df = self.root
        return df.cols.apply(input_cols, _port, output_cols=output_cols, meta_action=Actions.PORT.value,
                             mode="vectorized")

    def subdomain(self, input_cols, output_cols=None):
        # From https://www.hi-bumblebee.com:8080 it returns www
        def _subdomain(value):
            return value.str.extract(regex_full_url)[3]

        df = self.root
        return df.cols.apply(input_cols, _subdomain, output_cols=output_cols, meta_action=Actions.SUBDOMAIN.value,
                             mode="vectorized")

    # Email functions
    def email_username(self, input_cols, output_cols=None):
        def _email_user(value):
            return value.str.split('@')[0]

        df = self.root
        return df.cols.apply(input_cols, _email_user, output_cols=output_cols, meta_action=Actions.EMAIL_USER.value,
                             mode="vectorized")

    def email_domain(self, input_cols, output_cols=None):
        def _email_domain(value):
            # return value.str.split('@').str[1]
            # return value.str.extract(r"@(.*)")
            return value.str.split('@')[1]

        df = self.root
        return df.cols.apply(input_cols, _email_domain, output_cols=output_cols, meta_action=Actions.EMAIL_DOMAIN.value,
                             mode="vectorized")

    # Mask functions
    def missing(self, input_cols, output_cols=None):
        return self.append(self.root.mask.missing())
