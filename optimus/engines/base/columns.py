import re
import string
import time
from abc import abstractmethod, ABC
from functools import reduce
from typing import Union

import jellyfish as jellyfish
import nltk
import numpy as np
import pandas as pd
import pydateinfer
import wordninja
from dask import dataframe as dd
from glom import glom
from metaphone import doublemetaphone
from multipledispatch import dispatch
from nltk import LancasterStemmer
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.stem import SnowballStemmer
from num2words import num2words
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer

# from optimus.engines.dask.functions import DaskFunctions as F
from optimus.engines.base.meta import Meta
from optimus.helpers.check import is_dask_dataframe
from optimus.helpers.columns import parse_columns, check_column_numbers, prepare_columns, get_output_cols, \
    validate_columns_names, name_col
from optimus.helpers.types import DataFrameType, InternalDataFrameType, DataFrameTypeList
from optimus.helpers.constants import RELATIVE_ERROR, ProfilerDataTypes, Actions, PROFILER_CATEGORICAL_DTYPES, \
    CONTRACTIONS
from optimus.helpers.converter import format_dict
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_dict, is_str, is_list_value, is_one_element, \
    is_list_of_tuples, is_int, is_list_of_str, is_tuple, is_null, is_list, str_to_int
from optimus.profiler.constants import MAX_BUCKETS

TOTAL_PREVIEW_ROWS = 30
CATEGORICAL_THRESHOLD = 0.10
ZIPCODE_THRESHOLD = 0.80
INFER_PROFILER_ROWS = 200


class BaseColumns(ABC):
    """Base class for all Cols implementations"""

    def __init__(self, root):
        self.root = root
        self.F = self.root.functions

    def _series_to_dict(self, series):
        return self._series_to_pandas(series).to_dict()

    def _series_to_dict_delayed(self, series):
        return series.to_dict()

    def _series_to_pandas(self, series):
        pass

    def _map(self, df, input_col, output_col, func, *args):
        return df[input_col].apply(func, args=(*args,))

    def _names(self):
        pass

    def _transformed(self, updated=[]):
        actions = Meta.get(self.root.meta, "transformations.actions") or []
        transformed_columns = []
        updated = val_to_list(updated)

        for action in actions:
            action_cols = action.get("columns", None)
            action_stats = action.get("updated_stats", [])
            
            if not action_cols:
                continue
            
            if is_tuple(action_cols):
                action_cols = action_cols[1]

            if len(updated) and all(stat in action_stats for stat in updated):
                continue
            
            action_cols = val_to_list(action_cols)
            transformed_columns += action_cols

        return list(set(transformed_columns))


    def _set_transformed_stat(self, cols="*", stats=None):

        cols = parse_columns(self.root, cols)
        actions = Meta.get(self.root.meta, "transformations.actions") 
        stats = val_to_list(stats)

        for i, action in enumerate(actions):
            action_cols = action.get("columns", None)
            action_stats = action.get("updated_stats", [])
            
            if not action_cols:
                continue
            
            if is_tuple(action_cols):
                action_cols = action_cols[1]

            action_cols = val_to_list(action_cols)

            if all(col in cols for col in action_cols):
                action.update({"updated_stats": list(set([*action_stats, *stats]))})

            actions[i] = action
        
        self.root.meta = Meta.set(self.root.meta, "transformations.actions", actions)

    @abstractmethod
    def append(self, dfs: DataFrameTypeList) -> DataFrameType:
        pass

    def select(self, cols="*", regex=None, data_type=None, invert=False, accepts_missing_cols=False) -> DataFrameType:
        """
        Select columns using index, column name, regex to data type
        :param cols:
        :param regex: Regular expression to filter the columns
        :param data_type: Data type to be filtered for
        :param invert: Invert the selection
        :param accepts_missing_cols:
        :return:
        """

        df = self.root
        cols = parse_columns(df, cols, is_regex=regex, filter_by_column_types=data_type, invert=invert,
                             accepts_missing_cols=accepts_missing_cols)
        meta = Meta.select_columns(df.meta, cols)
        dfd = df.data
        if cols is not None:
            dfd = dfd[cols]
        return self.root.new(dfd, meta=meta)

    def copy(self, cols="*", output_cols=None, columns=None) -> DataFrameType:
        """
        Copy one or multiple columns
        :param cols: Source column to be copied
        :param output_cols: Destination column
        :param columns: tuple of column [('column1','column_copy')('column1','column1_copy')()]
        :return:
        """
        df = self.root
        output_ordered_columns = df.cols.names()

        if columns is None:
            cols = parse_columns(df, cols)
            if is_list_value(cols) or is_one_element(cols):
                output_cols = get_output_cols(cols, output_cols)

        if columns:
            cols = list([c[0] for c in columns])
            output_cols = list([c[1] for c in columns])
            output_cols = get_output_cols(cols, output_cols)

        for input_col, output_col in zip(cols, output_cols):
            if input_col != output_col:
                col_index = output_ordered_columns.index(input_col) + 1
                output_ordered_columns[col_index:col_index] = [output_col]

        kw_columns = {}

        dfd = df.data
        meta = df.meta

        for input_col, output_col in zip(cols, output_cols):
            kw_columns[output_col] = dfd[input_col]
            meta = Meta.action(meta, Actions.COPY.value,
                               (input_col, output_col))

        df = self.root.new(dfd, meta=meta).cols.assign(kw_columns)

        return df.cols.select(output_ordered_columns)

    def drop(self, cols=None, regex=None, data_type=None) -> DataFrameType:
        """
        Drop a list of columns
        :param cols: Columns to be dropped
        :param regex: Regex expression to select the columns
        :param data_type:
        :return:
        """
        df = self.root
        if regex:
            r = re.compile(regex)
            cols = [c for c in list(df.cols.names()) if re.match(r, c)]

        cols = parse_columns(df, cols, filter_by_column_types=data_type)
        check_column_numbers(cols, "*")

        dfd = df.data.drop(columns=cols)
        meta = Meta.action(df.meta, Actions.DROP.value, cols)
        meta = Meta.drop_columns(meta, cols)

        return self.root.new(dfd, meta=meta)

    def keep(self, cols=None, regex=None) -> DataFrameType:
        """
        Drop a list of columns
        :param cols: Columns to be dropped
        :param regex: Regex expression to select the columns
        :return:
        """
        df = self.root
        dfd = df.data
        _cols = parse_columns(df, "*")
        if regex:
            # r = re.compile(regex)
            cols = [c for c in _cols if re.match(regex, c)]

        cols = parse_columns(df, cols)
        check_column_numbers(cols, "*")

        dfd = dfd.drop(columns=list(set(_cols) - set(cols)))

        df.meta = Meta.action(df.meta, Actions.KEEP.value, cols)

        return self.root.new(dfd, meta=df.meta)

    def word_count(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Count words by column element wise
        :param cols:
        :param output_cols:
        :return:
        """
        df = self.root
        dfd = df.data

        cols = parse_columns(df, cols)
        output_cols = get_output_cols(cols, output_cols)

        for input_col, output_col in zip(cols, output_cols):
            dfd[output_col] = dfd[input_col].str.split().str.len()
        return self.root.new(dfd)

    @staticmethod
    @abstractmethod
    def to_timestamp(cols, date_format=None, output_cols=None):
        pass

    def apply(self, cols="*", func=None, func_return_type=None, args=None, func_type=None, where=None,
              filter_col_by_data_types=None, output_cols=None, skip_output_cols_processing=False,
              meta_action=Actions.APPLY_COLS.value, mode="vectorized", set_index=False, default=None, **kwargs) -> DataFrameType:

        columns = prepare_columns(self.root, cols, output_cols, filter_by_column_types=filter_col_by_data_types,
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

        if is_str(func):
            _func = getattr(df.functions, func, False)

            if not _func:
                raise NotImplementedError(f"\"{func}\" is not available using {type(df).__name__}")
            else:
                func = _func

        for input_col, output_col in columns:
            if mode == "vectorized":
                # kw_columns[output_col] = self.F.delayed(func)(part, *args)
                kw_columns[output_col] = func(dfd[input_col], *args)

            elif mode == "partitioned":

                partitions = self.F.to_delayed(dfd[input_col])
                delayed_parts = [self.F.delayed(func)(
                    part, *args) for part in partitions]
                kw_columns[output_col] = self.F.from_delayed(delayed_parts)

            elif mode == "map":
                kw_columns[output_col] = self._map(
                    dfd, input_col, str(output_col), func, *args)

            # Preserve column order
            if output_col not in self.names():
                col_index = output_ordered_columns.index(input_col) + 1
                output_ordered_columns[col_index:col_index] = [output_col]

            meta = Meta.action(meta, meta_action, output_col)

        if set_index is True and mode != "partitioned":
            dfd = dfd.reset_index()

        df = self.root.new(dfd, meta=meta)

        if kw_columns:

            df = df.cols.assign(kw_columns)
        # Dataframe to Optimus dataframe
        df = df.cols.select(output_ordered_columns)

        return df

    def apply_by_data_types(self, cols="*", func=None, args=None, data_type=None):
        """
        Apply a function using pandas udf or udf if apache arrow is not available
        :param cols: Columns in which the function is going to be applied
        :param func: Functions to be applied to a columns
        :param args:
        :param func_type: pandas_udf or udf. If none try to use pandas udf (Pyarrow needed)
        :param data_type:
        :return:
        """

        cols = parse_columns(self.root, cols)

        mask = self.root.mask.match_data_type(cols, data_type)

        return self.set(cols, func=func, args=args, where=mask)

    def set(self, cols="*", value=None, where=None, args=[], default=None, eval_value=False):
        """
        Set a column value using a number, string or a expression.
        :param col_name:
        :param value: expression, function or value
        :param where: mask
        :param default: value
        :param eval_value:
        :return:
        """
        df = self.root
        dfd = df.data

        cols = val_to_list(cols)

        assign_dict = {}

        if where is not None:
            where = where.get_series()

        move_cols = []

        for col_name in cols:

            temp_col_name = name_col(col_name, "SET")

            if default is not None:
                if is_str(default) and default in df.cols.names():
                    if default != col_name:
                        move_cols.append((default, col_name))
                    default = dfd[default]
                elif isinstance(default, self.root.__class__):
                    default = default.get_series()
                else:
                    dfd[temp_col_name] = default
                    default = dfd[temp_col_name]
                    del dfd[temp_col_name]
            elif col_name:
                if col_name in df.cols.names():
                    default = dfd[col_name]
                else:
                    default = None
            if eval_value and is_str(value):
                value = eval(value)

            if is_str(where):
                if where in df.cols.names():
                    where = df[where]
                else:
                    where = eval(where)

            if callable(value):
                args = val_to_list(args)
                value = value(default, *args)

            if where is not None:
                if isinstance(value, self.root.__class__):
                    value = value.get_series()
                else:
                    # TO-DO: Create the value series
                    dfd[temp_col_name] = value
                    value = dfd[temp_col_name]
                    del dfd[temp_col_name]

                value = default.mask(where, value)

            else:
                if isinstance(value, self.root.__class__):
                    value = value.data[value.cols.names()[0]]

            assign_dict[col_name] = value

        # meta = Meta.action(df.meta, Actions.SET.value, col_name)
        new_df = self.root.new(df.data).cols.assign(assign_dict)
        for col, new_col in move_cols:
            new_df = new_df.cols.move(new_col, "after", col)
        return new_df

    @dispatch(object, object)
    def rename(self, columns_old_new=None, func=None) -> DataFrameType:
        """"
        Changes the name of a column(s) dataFrame.
        :param columns_old_new: List of tuples or list of strings. Each tuple has de following form: (oldColumnName, newColumnName).
        :param func: can be lower, upper or any string transformation function
        """

        df = self.root
        dfd = df.data
        meta = df.meta

        # Apply a transformation function
        if is_list_of_tuples(columns_old_new):
            validate_columns_names(df, columns_old_new)
        elif is_list_of_str(columns_old_new):
            validate_columns_names(df, columns_old_new)
            columns_old_new = [(col, col) for col in columns_old_new]
        else:
            columns_old_new = [(col, col) for col in df.cols.names()]

        for col_name in columns_old_new:

            old_col_name = col_name[0]
            new_col_name = col_name[1]

            if is_int(old_col_name):
                old_col_name = df.cols.names()[old_col_name]
            if func:
                new_col_name = func(new_col_name)

            # DaskColumns.set_meta(col_name, "optimus.transformations", "rename", append=True)
            # TODO: this seems to the only change in this function compare to pandas. Maybe this can
            #  be moved to a base class

            if old_col_name != new_col_name:
                dfd = dfd.rename(columns={old_col_name: new_col_name})
                meta = Meta.action(meta, Actions.RENAME.value,
                                   (old_col_name, new_col_name))

        return self.root.new(dfd, meta=meta)

    @dispatch(list)
    def rename(self, columns_old_new=None) -> DataFrameType:
        return self.rename(columns_old_new, None)

    @dispatch(object)
    def rename(self, func=None) -> DataFrameType:
        return self.rename(None, func)

    @dispatch(str, str, object)
    def rename(self, old_column, new_column, func=None) -> DataFrameType:
        return self.rename([(old_column, new_column)], func)

    @dispatch(str, str)
    def rename(self, old_column, new_column) -> DataFrameType:
        return self.rename([(old_column, new_column)], None)

    def parse_inferred_types(self, col_data_type):
        """
        Parse a data type to a profiler data type
        :return:
        """
        df = self.root
        columns = {}
        for k, v in col_data_type.items():
            # Initialize values to 0
            result_default = {
                data_type: 0 for data_type in df.constants.DTYPES_TO_INFERRED.keys()}
            for k1, v1 in v.items():
                for k2, v2 in df.constants.DTYPES_TO_INFERRED.items():
                    if k1 in df.constants.DTYPES_TO_INFERRED[k2]:
                        result_default[k2] = result_default[k2] + v1
            columns[k] = result_default
        return columns

    def inferred_types(self, columns="*"):
        """
        Get the profiler data types from the meta data
        :param columns:
        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)
        result = {}

        for col_name in columns:
            column_meta = Meta.get(
                df.meta, f"profile.columns.{col_name}.stats.inferred_type.data_type")
            result.update({col_name: column_meta})
        return result

    @dispatch(str, str)
    def set_data_type(self, column, data_type) -> DataFrameType:
        return self.set_data_type({column: data_type}, False)

    @dispatch(str, str, bool)
    def set_data_type(self, column, data_type, categorical) -> DataFrameType:
        return self.set_data_type({column: {"data_type": data_type, "categorical": categorical}}, False)

    @dispatch(dict)
    def set_data_type(self, columns: dict) -> DataFrameType:
        return self.set_data_type(columns, False)

    @dispatch(dict, bool)
    def set_data_type(self, columns: dict, inferred=False) -> DataFrameType:
        """
        Set profiler data type
        :param columns: A dict with the form {"col_name": profiler datatype}
        :param inferred: Wherter it was inferred or not
        :return:
        """
        df = self.root

        for col_name, element in columns.items():
            props = element if is_dict(element) else {"data_type": element}
            dtype = props["data_type"]
            dtype = df.constants.INFERRED_DTYPES_ALIAS.get(dtype, dtype)
            if dtype in ProfilerDataTypes.list():
                if not inferred:
                    df.meta = Meta.set(
                        df.meta, f"columns_data_types.{col_name}", props)
                df.meta = Meta.set(
                    df.meta, f"profile.columns.{col_name}.stats.inferred_type", props)
                df.meta = Meta.action(
                    df.meta, Actions.INFERRED_TYPE.value, col_name)
            else:
                RaiseIt.value_error(dtype, ProfilerDataTypes.list())

        return df

    def unset_data_type(self, columns="*"):
        """
        Unset profiler data type
        :param columns:
        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)

        for col_name in columns:
            props = Meta.get(df.meta, f"columns_data_types.{col_name}")

            if props is not None:
                df.meta = Meta.reset(df.meta, f"columns_data_types.{col_name}")
                df.meta = Meta.action(
                    df.meta, Actions.INFERRED_TYPE.value, col_name)

        return df

    def cast(self, cols=None, data_type=None, output_cols=None, columns=None) -> DataFrameType:
        """
        NOTE: We have two ways to cast the data. Use the use the native .astype() this is faster but can not handle some
        trnasformation like string to number in which should output nan.

        is pendulum faster than pd.to_datatime
        We could use astype str and boolean


        Cast the elements inside a column or a list of columns to a specific data type.
        Unlike 'cast' this not change the columns data type

        :param cols: Columns names to be casted
        :param output_cols:
        :param data_type: final data type
        :param columns: List of tuples of column names and types to be casted. This variable should have the
                following structure:
                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]
                The first parameter in each tuple is the column name, the second is the final datatype of column after
                the transformation is made.
        """

        df = self.root

        if columns is None:
            columns = prepare_columns(df, cols, output_cols, args=data_type)

        func_map = {
            "float": "to_float",
            "int": "to_integer",
            "datetime": "to_datetime",
            "bool": "to_boolean",
            "str": "to_string"
        }

        for item in columns:

            if len(item) == 3:
                input_col, output_col, arg = item
            elif len(item) == 2:
                input_col, arg = item
                output_col = input_col
            else:
                RaiseIt.value_error(columns, ["list of tuples"])

            if arg in func_map.keys():
                df = getattr(df.cols, func_map[arg])(input_col, output_col)
            else:
                RaiseIt.value_error(arg, list(func_map.keys()))

        return df

    @staticmethod
    @abstractmethod
    def astype(*args, **kwargs):
        pass

    def profile(self, cols="*", bins: int = MAX_BUCKETS, flush: bool = False) -> dict:
        """
        Returns the profile of selected columns
        :param cols: Columns to get the profile from
        :param bins: Number of buckets
        :param flush: Flushes the cache of the whole profile to process it again
        """
        # Uses profile on self instead of calculate_profile to get the data only when it's neccessary
        self.root.profile(cols=cols, bins=bins, flush=flush)
        df = self.root

        return df.profile.columns(cols)

    def pattern(self, cols="*", output_cols=None, mode=0) -> DataFrameType:
        """
        Replace alphanumeric and punctuation chars for canned chars. We aim to help to find string patterns
        c = Any alpha char in lower or upper case
        l = Any alpha char in lower case
        U = Any alpha char in upper case
        * = Any alphanumeric in lower or upper case. Used only in type 2 nd 3
        # = Any numeric
        ! = Any punctuation

        :param cols:
        :param output_cols:
        :param mode:
        0: Identify lower, upper, digits. Except spaces and special chars.
        1: Identify chars, digits. Except spaces and special chars
        2: Identify Any alphanumeric. Except spaces and special chars
        3: Identify alphanumeric and special chars. Except white spaces
        :return:
        """

        df = self.root
        columns = prepare_columns(df, cols, output_cols)

        def split(word):
            return [char for char in word]

        alpha_lower = split(string.ascii_lowercase)
        alpha_upper = split(string.ascii_uppercase)
        digits = split(string.digits)
        punctuation = split(string.punctuation)

        if mode == 0:
            search_by = alpha_lower + alpha_upper + digits
            replace_by = ["l"] * len(alpha_lower) + ["U"] * \
                len(alpha_upper) + ["#"] * len(digits)
        elif mode == 1:
            search_by = alpha_lower + alpha_upper + digits
            replace_by = ["c"] * len(alpha_lower) + ["c"] * \
                len(alpha_upper) + ["#"] * len(digits)
        elif mode == 2:
            search_by = alpha_lower + alpha_upper + digits
            replace_by = ["*"] * len(alpha_lower + alpha_upper + digits)
        elif mode == 3:
            search_by = alpha_lower + alpha_upper + digits + punctuation
            replace_by = ["*"] * \
                len(alpha_lower + alpha_upper + digits + punctuation)
        else:
            RaiseIt.value_error(mode, ["0", "1", "2", "3"])

        kw_columns = {}

        for input_col, output_col in columns:
            kw_columns[output_col] = df.cols.select(input_col).cols.to_string().cols.normalize_chars().cols.replace(
                search=search_by, replace_by=replace_by).data[input_col]

        return df.cols.assign(kw_columns)

    def assign(self, kw_columns):

        if kw_columns.__class__ == self.root.__class__:
            kw_columns = {name: kw_columns.data[name]
                          for name in kw_columns.cols.names()}

        for key in kw_columns:
            if kw_columns[key].__class__ == self.root.__class__:
                name = kw_columns[key].cols.names()[0]
                kw_columns[key] = kw_columns[key].cols.rename([(name, key)])
                kw_columns[key] = kw_columns[key].data[key]

        meta = Meta.action(self.root.meta, Actions.SET.value,
                           list(kw_columns.keys()))

        return self.root.new(self.root._assign(kw_columns), meta=meta)

    # TODO: Consider implement lru_cache for caching
    def calculate_pattern_counts(self, cols="*", n=10, mode=0, flush=False) -> DataFrameType:
        """
        Counts how many equal patterns there are in a column. Uses a cache to trigger the operation only if necessary. 
        Saves the result to meta and returns the same dataframe
        :param cols:
        :param n: Top n matches
        :param mode:
        :param flush: Flushes the cache to process again
        :return:
        """

        df = self.root

        result = {}
        cols = parse_columns(df, cols)
        for input_col in cols:
            column_modified_time = Meta.get(
                df.meta, f"profile.columns.{input_col}.modified")
            patterns_update_time = Meta.get(
                df.meta, f"profile.columns.{input_col}.patterns.updated")
            if column_modified_time is None:
                column_modified_time = -1
            if patterns_update_time is None:
                patterns_update_time = 0

            patterns_more = Meta.get(
                df.meta, f"profile.columns.{input_col}.patterns.more")

            if column_modified_time > patterns_update_time \
                    or patterns_update_time == 0 \
                    or flush is True \
                    or patterns_more:

                # Plus n + 1 so we can could let the user know if there are more patterns
                result[input_col] = \
                    df.cols.pattern(input_col, mode=mode).cols.frequency(input_col, n=n + 1)["frequency"][
                        input_col]

                if len(result[input_col]["values"]) > n:
                    result[input_col].update({"more": True})

                    # Remove extra element from list
                    result[input_col]["values"].pop()

                df.meta = Meta.set(
                    df.meta, f"profile.columns.{input_col}.patterns", result[input_col])
                df.meta = Meta.set(
                    df.meta, f"profile.columns.{input_col}.patterns.updated", time.time())

            else:
                result[input_col] = Meta.get(
                    df.meta, f"profile.columns.{input_col}.patterns")

        return df

    def correlation(self, cols="*", method="pearson", tidy=True):
        """

        :param cols:
        :param method:
        :param tidy:
        :return:
        """
        df = self.root
        dfd = self.root.data
        cols = parse_columns(df, cols)

        result = dfd[cols].corr(method).to_dict()

        if tidy and is_list(cols) and len(cols) == 2:
            result = result[cols[0]][cols[1]]

        return result

    def crosstab(self, col_x, col_y, output="dict") -> dict:
        pass

    def pattern_counts(self, cols="*", n=10, mode=0, flush=False) -> dict:
        """
        Get how many equal patterns there are in a column. Triggers the operation only if necessary
        :param cols:
        :param n: Top n matches
        :param mode:
        :param flush: Flushes the cache to process again
        :return:
        """

        df = self.root

        result = {}
        cols = parse_columns(df, cols)

        calculate = flush

        for input_col in cols:
            patterns_values = Meta.get(
                df.meta, f"profile.columns.{input_col}.patterns.values")
            patterns_more = Meta.get(
                df.meta, f"profile.columns.{input_col}.patterns.more")

            if patterns_values is None or (len(patterns_values) < n and patterns_more):
                calculate = True
                break

            column_modified_time = Meta.get(
                df.meta, f"profile.columns.{input_col}.modified")
            patterns_update_time = Meta.get(
                df.meta, f"profile.columns.{input_col}.patterns.updated")
            if column_modified_time is None:
                column_modified_time = -1
            if patterns_update_time is None:
                patterns_update_time = 0

            if column_modified_time > patterns_update_time or patterns_update_time == 0:
                calculate = True
                break

        if calculate:
            df = df.cols.calculate_pattern_counts(cols, n, mode, flush)
            profile = Meta.get(df.meta, "profile")
            self.meta = df.meta

        for input_col in cols:
            result[input_col] = Meta.get(
                df.meta, f"profile.columns.{input_col}.patterns")
            if len(result[input_col]["values"]) > n:
                result[input_col].update({"more": True})
                result[input_col]["values"] = result[input_col]["values"][0:n]

        return result

    def groupby(self, by, agg) -> DataFrameType:
        """
        This helper function aims to help managing columns name in the aggregation output.
        Also how to handle ordering columns because dask can order columns
        :param by: Column names
        :param agg:
        :return:
        """
        df = self.root.data
        compact = {}
        for col_agg in list(agg.values()):
            for col_name, _agg in col_agg.items():
                compact.setdefault(col_name, []).append(_agg)

        df = df.groupby(by=by).agg(compact).reset_index()
        df.columns = (val_to_list(by) + val_to_list(list(agg.keys())))
        df = self.root.new(df)
        return df

    # def is_match(self, cols="*", data_type, invert=False):
    #     """
    #     Find the rows that match a data type
    #     :param columns:
    #     :param data_type: data type to match
    #     :param invert: Invert the match
    #     :return:
    #     """
    #     df = self.root
    #     dfd = df.data
    #     columns = parse_columns(df, columns)
    #
    #     f = inferred_type_func(data_type)
    #     if f is not None:
    #         for col_name in columns:
    #             dfd = dfd[col_name].apply(f)
    #             dfd = ~dfd if invert is True else dfd
    #     return self.root.new(dfd)

    def move(self, column, position, ref_col=None) -> DataFrameType:
        """
        Move a column to specific position
        :param column: Column to be moved
        :param position: Column new position. Accepts 'after', 'before', 'beginning', 'end'
        :param ref_col: Column taken as reference
        :return: DataFrame
        """
        df = self.root
        # Check that column is a string or a list
        column = parse_columns(df, column)

        # Get dataframe columns
        all_columns = df.cols.names()

        # Get source and reference column index position
        if ref_col:
            ref_col = parse_columns(df, ref_col)
            new_index = all_columns.index(ref_col[0])
            old_index = all_columns.index(column[0])
            left = -1 if new_index > old_index else 0
        else:
            new_index = all_columns
        # Column to move

        if position == 'after':
            # Check if the movement is from right to left:
            new_index = new_index + 1 + left
        elif position == 'before':
            new_index = new_index + left
        elif position == 'beginning':
            new_index = 0
        elif position == 'end':
            new_index = len(all_columns)
        else:
            RaiseIt.value_error(
                position, ["after", "before", "beginning", "end"])

        # Remove
        new_columns = []
        for col_name in column:
            new_columns.append(all_columns.pop(
                all_columns.index(col_name)))  # delete
        # Move the column to the new place
        for col_name in new_columns[::-1]:
            # insert and delete a element
            all_columns.insert(new_index, col_name)
            # new_index = new_index + 1
        return df[all_columns]

    def sort(self, order: Union[str, list] = "asc", columns=None) -> DataFrameType:
        """
        Sort data frames columns in asc or desc order
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

    def data_types(self, columns="*") -> dict:
        """
        Return the column(s) data type as string
        :param columns: Columns to be processed
        :return: {col_name: data_type}
        """
        df = self.root
        columns = parse_columns(df, columns)
        data_types = ({k: str(v) for k, v in dict(df.data.dtypes).items()})
        return {col_name: data_types[col_name] for col_name in columns}

    def schema_data_type(self, columns="*"):
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
                result[col_name] = dfd[col_name].dtype.name
        return format_dict(result)

    def agg_exprs(self, cols="*", funcs=None, *args, compute=True, tidy=True, parallel=False):
        """
        Create and run aggregation
        :param cols: Column over with to apply the aggregations
        :param funcs: Aggregation list
        :param args:Aggregations params
        :param compute: Compute the result or return a delayed function
        :param tidy: compact the dict output
        :param parallel: Execute the function in every column or apply it over the whole dataframe
        :return:
        """
        df = self.root
        cols = parse_columns(df, cols)

        if args is None:
            args = []
        elif not is_tuple(args, ):
            args = (args,)

        funcs = val_to_list(funcs)

        if parallel:
            all_funcs = [getattr(df[cols].data, func.__name__)()
                         for func in funcs]
            agg_result = {func.__name__: self.exec_agg(
                all_funcs, compute) for func in funcs}

        else:
            agg_result = {func.__name__: {col_name: self.exec_agg(func(df.data[col_name], *args), compute) for
                                          col_name in cols} for func in funcs}
            # agg_result = [{func.__name__: {self.exec_agg({col_name: func(df.data[col_name], *args)}, compute)}} for
            #               col_name in cols for func in funcs]

            result = {}

        # # Reformat aggregation
        # for agg in agg_result:
        #     print("agg",agg)
        #     for x, y in agg.items():
        #         result.setdefault(x, {}).update(y)

        return format_dict(agg_result, tidy)

    @staticmethod
    def exec_agg(exprs, compute):
        while isinstance(exprs, (list, tuple)):
            exprs = exprs[0]
        if getattr(exprs, "to_dict", None):
            exprs = exprs.to_dict()
        return exprs

    def mad(self, cols="*", relative_error=RELATIVE_ERROR, more=False, tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.mad, relative_error, more, compute=compute, tidy=tidy)

    def min(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.min, compute=compute, tidy=tidy, parallel=False)

    def max(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.max, compute=compute, tidy=tidy, parallel=False)

    def mode(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.mode, compute=compute, tidy=tidy)

    def range(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.range, compute=compute, tidy=tidy)

    def percentile(self, cols="*", values=None, relative_error=RELATIVE_ERROR, tidy=True, compute=True):
        df = self.root

        if values is None:
            values = [0.25, 0.5, 0.75]
        return df.cols.agg_exprs(cols, self.F.percentile, values, relative_error, tidy=tidy, compute=True)

    def median(self, cols="*", relative_error=RELATIVE_ERROR, tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.percentile, [0.5], relative_error, tidy=tidy, compute=True)

    # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
    def kurtosis(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.kurtosis, tidy=tidy, compute=compute)

    def skew(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.skew, tidy=tidy, compute=compute)

    def mean(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.mean, tidy=tidy, compute=compute)

    def sum(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.sum, tidy=tidy, compute=compute)

    def cumsum(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.cumsum, tidy=tidy, compute=compute)

    def cumprod(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.cumprod, tidy=tidy, compute=compute)

    def cummax(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.new(df.cols.agg_exprs(cols, self.F.cummax, tidy=tidy, compute=compute))

    def cummin(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.cummin, tidy=tidy, compute=compute)

    def var(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.var, tidy=tidy, compute=compute)

    def std(self, cols="*", tidy=True, compute=True):
        df = self.root
        return df.cols.agg_exprs(cols, self.F.std, tidy=tidy, compute=compute)

    def item(self, cols="*", n=None, output_cols=None, mode="list") -> DataFrameType:
        """
        Get items from list
        :param cols:
        :param n:
        :param output_cols:
        :param mode:
        :return:
        """

        def func(value, keys):
            return value.str[keys]

        return self.apply(cols, func, args=(n,), output_cols=output_cols, meta_action=Actions.ITEM.value,
                          mode="vectorized")

    def get(self, cols="*", keys=None, output_cols=None) -> DataFrameType:
        """
        Get items from dicts
        :param cols:
        :param keys:
        :param output_cols:
        :param mode:
        :return:
        """

        def func(value, keys):
            return glom(value, keys, skip_exc=KeyError)

        return self.apply(cols, func, args=(keys,), output_cols=output_cols, meta_action=Actions.GET.value,
                          mode="map")

    # Math Operations
    def abs(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply abs to column
        :param cols:
        :param output_cols:
        :return:
        """

        return self.apply(cols, self.F.abs, output_cols=output_cols, meta_action=Actions.ABS.value,
                          mode="vectorized")

    def exp(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Returns Euler's number, e (~2.718) raised to a power.
        :param cols:
        :param output_cols:
        :return:
        """

        return self.apply(cols, self.F.exp, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized")

    def mod(self, cols="*", divisor=2, output_cols=None) -> DataFrameType:
        """
        Apply mod to column
        :param cols:
        :param divisor:
        :param output_cols:
        :return:(
        """

        return self.apply(cols, self.F.mod, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized", args=divisor)

    def log(self, cols="*", base=10, output_cols=None) -> DataFrameType:
        """
        Apply mod to column
        :param cols:
        :param base:
        :param output_cols:
        :return:(
        """

        return self.apply(cols, self.F.log, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized", args=base)

    def ln(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply mod to column
        :param cols:
        :param output_cols:
        :return:(
        """

        return self.apply(cols, self.F.ln, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized")

    def pow(self, cols="*", power=2, output_cols=None) -> DataFrameType:
        """
        Apply mod to column
        :param power:
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.pow, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized", args=power)

    def sqrt(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply sqrt to column
        :param cols:
        :param output_cols:
        :return:(
        """

        return self.apply(cols, self.F.sqrt, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized")

    def reciprocal(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply sqrt to column
        :param cols:
        :param output_cols:
        :return:(
        """

        return self.apply(cols, self.F.reciprocal, output_cols=output_cols, meta_action=Actions.MATH.value,
                          mode="vectorized")

    def _round(self, cols="*", mode=True, output_cols=None) -> DataFrameType:

        df = self.root

        if is_int(mode):
            df = df.cols.round(cols, decimals=mode, output_cols=output_cols)
        else:
            modes = {
                "floor": "floor",
                "down": "floor",
                "ceil": "ceil",
                "up": "ceil",
                "round": "round",
                True: "round"
            }

            if not mode in modes:
                RaiseIt.value_error(mode, list(modes.keys()))

            df = getattr(df.cols, modes[mode])(cols, output_cols=output_cols)

        return df

    def round(self, cols="*", decimals=0, output_cols=None) -> DataFrameType:
        """

        :param cols:
        :param decimals:
        :param output_cols:
        :return:
        """
        df = self.root
        return df.cols.apply(cols, self.F.round, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized", args=decimals)

    def floor(self, cols="*", output_cols=None) -> DataFrameType:
        """

        :param cols:
        :param output_cols:
        :return:
        """
        df = self.root
        return df.cols.apply(cols, self.F.floor, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def ceil(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply ceil to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.ceil, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    # Trigonometric
    def sin(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply sin to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.sin, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def cos(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply cos to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.cos, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def tan(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply cos to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.tan, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def asin(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply cos to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.asin, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def acos(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply cos to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.acos, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def atan(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply cos to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.atan, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def sinh(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply sin to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.sinh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def cosh(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply cos to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.cosh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def tanh(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply cos to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.tanh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def asinh(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply sin to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.asinh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def acosh(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply cos to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.acosh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def atanh(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Apply cos to column
        :param cols:
        :param output_cols:
        :return:(
        """

        df = self.root
        return df.cols.apply(cols, self.F.atanh, output_cols=output_cols, meta_action=Actions.MATH.value,
                             mode="vectorized")

    def extract(self, cols="*", regex=None, output_cols=None) -> DataFrameType:

        return self.apply(cols, self.F.extract, args=(regex,), func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.EXTRACT.value, mode="vectorized")

        # def replace_regex(cols, regex=None, value=None, output_cols=None):

    def slice(self, cols="*", start=None, stop=None, step=None, output_cols=None) -> DataFrameType:
        def _slice(value, _start, _stop, _step):
            return self.F.slice(value, _start, _stop, _step)

        return self.apply(cols, _slice, args=(start, stop, step), func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.SLICE.value, mode="vectorized")

    def left(self, cols="*", n=None, output_cols=None) -> DataFrameType:

        df = self.apply(cols, self.F.left, args=(n,), func_return_type=str,
                        output_cols=output_cols, meta_action=Actions.LEFT.value, mode="vectorized")
        return df

    def right(self, cols="*", n=None, output_cols=None) -> DataFrameType:
        df = self.apply(cols, self.F.right, args=(n,), func_return_type=str,
                        output_cols=output_cols, meta_action=Actions.RIGHT.value, mode="vectorized")
        return df

    def mid(self, cols="*", start=0, n=1, output_cols=None) -> DataFrameType:
        df = self.apply(cols, self.F.mid, args=(start, n), func_return_type=str,
                        output_cols=output_cols, meta_action=Actions.MID.value, mode="vectorized")
        return df

    def to_float(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, "to_float", func_return_type=float,
                          output_cols=output_cols, meta_action=Actions.TO_FLOAT.value, mode="vectorized")

    def to_integer(self, cols="*", default=0, output_cols=None) -> DataFrameType:
        return self.apply(cols, "to_integer", args=(default,), func_return_type=int,
                          output_cols=output_cols, meta_action=Actions.TO_INTEGER.value, mode="vectorized")

    def to_boolean(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, "to_boolean", func_return_type=int,
                          output_cols=output_cols, meta_action=Actions.TO_BOOLEAN.value, mode="vectorized")

    def to_string(self, cols="*", output_cols=None) -> DataFrameType:
        filtered_columns = []
        df = self.root

        cols = parse_columns(df, cols)
        for col_name in cols:
            dtype = df.cols.data_types(col_name)

            if dtype != np.object:
                filtered_columns.append(col_name)

        if len(filtered_columns) > 0:
            return self.apply(cols, "to_string", func_return_type=str,
                              output_cols=output_cols, meta_action=Actions.TO_STRING.value, mode="vectorized",
                              func_type="column_expr")
        else:
            return df

    def lower(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, self.F.lower, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.LOWER.value, mode="vectorized", func_type="column_expr")

    def infer_data_types(self, cols="*", output_cols=None) -> DataFrameType:
        dtypes = self.root[cols].cols.data_types()
        return self.apply(cols, self.F.infer_data_types, args=(dtypes,), func_return_type=str,
                          output_cols=output_cols,
                          meta_action=Actions.INFER.value, mode="map", func_type="column_expr")

    def upper(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, self.F.upper, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.UPPER.value, mode="vectorized", func_type="column_expr")

    def title(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, self.F.title, func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.PROPER.value, mode="vectorized",
                          func_type="column_expr")

    def capitalize(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, self.F.capitalize, func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.PROPER.value, mode="vectorized",
                          func_type="column_expr")

    def proper(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, self.F.proper, func_return_type=str,
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

    def pad(self, cols="*", width=0, fillchar="0", side="left", output_cols=None) -> DataFrameType:

        return self.apply(cols, self.F.pad, args=(width, side, fillchar,), func_return_type=str,
                          output_cols=output_cols,
                          meta_action=Actions.PAD.value, mode="vectorized")

    def trim(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, self.F.trim, func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.TRIM.value, mode="vectorized")

    def strip_html(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, self.F.strip_html, func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.TRIM.value, mode="map")

    def date_format(self, cols="*", current_format=None, output_format=None, output_cols=None) -> DataFrameType:

        return self.apply(cols, self.F.date_format, args=(current_format, output_format), func_return_type=str,
                          output_cols=output_cols, meta_action=Actions.DATE_FORMAT.value, mode="partitioned",
                          set_index=False)

    def word_tokenizer(self, cols="*", output_cols=None) -> DataFrameType:

        return self.apply(cols, self.F.word_tokenize, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.WORD_TOKENIZE.value, mode="map")

    def word_count(self, cols="*", output_cols=None) -> DataFrameType:

        df = self.root

        cols = parse_columns(df, cols)
        output_cols = get_output_cols(cols, output_cols)

        return df.cols.word_tokenizer(cols, output_cols).cols.len(output_cols)

    def len(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, self.F.len, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.LENGTH.value, mode="vectorized")

    def expand_contrated_words(self, cols="*", output_cols=None) -> DataFrameType:
        i, j = zip(*CONTRACTIONS)
        df = self.replace(cols, i, j, search_by="words")
        return df

    @staticmethod
    @abstractmethod
    def reverse(cols="*", output_cols=None) -> DataFrameType:
        pass

    def remove(self, cols="*", search=None, search_by="chars", output_cols=None) -> DataFrameType:
        return self.replace(cols=cols, search=search, replace_by="", search_by=search_by,
                            output_cols=output_cols)

    def normalize_chars(self, cols="*", output_cols=None):
        """
        Remove diacritics from a dataframe
        :param cols:
        :param output_cols:
        :return:
        """

        return self.apply(cols, self.F.normalize_chars, func_return_type=str,
                          meta_action=Actions.REMOVE_ACCENTS.value,
                          output_cols=output_cols, mode="vectorized")

    def remove_numbers(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Remove numbers from a dataframe
        :param cols:
        :param output_cols:
        :return:
        """

        def _remove_numbers(value):
            return value.astype(str).str.replace(r'\d+', '')

        return self.apply(cols, _remove_numbers, func_return_type=str,
                          output_cols=output_cols, mode="vectorized", set_index=True)

    def remove_white_spaces(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Remove all white spaces from a dataframe
        :param cols:
        :param output_cols:
        :return:
        """

        return self.apply(cols, self.F.remove_white_spaces, func_return_type=str,
                          output_cols=output_cols, mode="vectorized")

    def remove_stopwords(self, cols="*", language="english", output_cols=None) -> DataFrameType:
        """
        Remove extra whitespace between words and trim whitespace from the beginning and the end of each string.
        :param cols:
        :param language: specify the stopwords language
        :param output_cols:
        :return:
        """

        stop = stopwords.words(language)
        df = self.root

        cols = parse_columns(df, cols)
        output_cols = get_output_cols(cols, output_cols)

        return df.cols.lower(cols, output_cols).cols.replace(output_cols, stop, "", "words").cols.normalize_spaces(output_cols)

    def remove_urls(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, self.F.remove_urls, func_return_type=str,
                          output_cols=output_cols, mode="vectorized")

    def normalize_spaces(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Remove extra whitespace between words and trim whitespace from the beginning and the end of each string.
        :param cols:
        :param output_cols:
        :return:
        """

        return self.apply(cols, self.F.normalize_spaces, func_return_type=str,
                          output_cols=output_cols, mode="vectorized")

    def remove_special_chars(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Remove special chars from a dataframe
        :param cols:
        :param output_cols:
        :return:
        """
        df = self.root
        return df.cols.replace(cols, [s for s in string.punctuation], "", "chars", output_cols=output_cols)

    def to_datetime(self, cols="*", format=None, output_cols=None) -> DataFrameType:
        """

        :param cols:
        :param format:
        :param output_cols:
        :return:
        """
        return self.apply(cols, "to_datetime", func_return_type=str,
                          output_cols=output_cols, args=format, mode="partitioned")

    def year(self, cols="*", format=None, output_cols=None) -> DataFrameType:
        """

        :param cols:
        :param format:
        :param output_cols:
        :return:
        """

        return self.apply(cols, self.F.year, args=format, output_cols=output_cols,
                          meta_action=Actions.YEAR.value,
                          mode="vectorized", set_index=True)

    def month(self, cols="*", format=None, output_cols=None) -> DataFrameType:
        """

        :param cols:
        :param format:
        :param output_cols:
        :return:
        """

        return self.apply(cols, self.F.month, args=format, output_cols=output_cols, mode="vectorized",
                          set_index=True)

    def day(self, cols="*", format=None, output_cols=None) -> DataFrameType:

        return self.apply(cols, self.F.day, args=format, output_cols=output_cols, mode="vectorized",
                          set_index=True)

    def hour(self, cols="*", format=None, output_cols=None) -> DataFrameType:

        def _hour(value, _format):
            return self.F.hour(value, _format)

        return self.apply(cols, _hour, args=format, output_cols=output_cols, mode="vectorized", set_index=True)

    def minute(self, cols="*", format=None, output_cols=None) -> DataFrameType:

        def _minute(value, _format):
            return self.F.minute(value, _format)

        return self.apply(cols, _minute, args=format, output_cols=output_cols, mode="vectorized", set_index=True)

    def second(self, cols="*", format=None, output_cols=None) -> DataFrameType:

        def _second(value, _format):
            return self.F.second(value, _format)

        return self.apply(cols, _second, args=format, output_cols=output_cols, mode="vectorized", set_index=True)

    def weekday(self, cols="*", format=None, output_cols=None) -> DataFrameType:

        def _second(value, _format):
            return self.F.weekday(value, _format)

        return self.apply(cols, _second, args=format, output_cols=output_cols, mode="vectorized", set_index=True)

    def _td_between(self, cols="*", func=None, value=None, date_format=None, round=None, output_cols=None) -> DataFrameType:

        df = self.root
        cols = parse_columns(df, cols)
        output_cols = get_output_cols(cols, output_cols)
        col_names = df.cols.names()

        if is_list(cols) and len(cols) == 2 and value is None:
            value = df.data[cols[1]]
            cols = cols[0]
        elif is_str(value) and value in col_names:
            value = df.data[value]
        elif is_list_of_str(value):
            value = [df.data[v] if v in col_names else v for v in value]

        def _years_between(series, args):
            return self.F.days_between(series, *args)/365.25

        return df.cols.apply(cols, func, args=[value, date_format], func_return_type=str,
                             output_cols=output_cols,
                             meta_action=Actions.YEARS_BETWEEN.value, mode="partitioned", set_index=True).cols._round(output_cols, round)

    def years_between(self, cols="*", value=None, date_format=None, round=None, output_cols=None) -> DataFrameType:

        def _years_between(series, args):
            return self.F.days_between(series, *args)/365.25

        return self._td_between(cols, _years_between, value, date_format, round, output_cols)

    def months_between(self, cols="*", value=None, date_format=None, round=None, output_cols=None) -> DataFrameType:

        def _months_between(series, args):
            return self.F.days_between(series, *args)/30.4375

        return self._td_between(cols, _months_between, value, date_format, round, output_cols)

    def days_between(self, cols="*", value=None, date_format=None, round=None, output_cols=None) -> DataFrameType:

        def _days_between(series, args):
            return self.F.days_between(series, *args)

        return self._td_between(cols, _days_between, value, date_format, round, output_cols)

    def replace(self, cols="*", search=None, replace_by=None, search_by="chars", ignore_case=False,
                output_cols=None) -> DataFrameType:
        """
        Replace a value, list of values by a specified string
        :param cols: '*', list of columns names or a single column name.
        :param search: Values to look at to be replaced
        :param replace_by: New value to replace the old one. Supports an array when searching by characters.
        :param search_by: Can be "full","words","chars" or "values".
        :param ignore_case: Ignore case when searching for match
        :param output_cols:
        :return: DataFrame
        """

        df = self.root

        if is_dict(cols):
            for col, replace in cols.items():
                _search = []
                _replace_by = []
                for replace_by, search in replace.items():
                    _replace_by.append(replace_by)
                    _search.append(search)
                df = df.cols._replace(
                    col, _search, _replace_by, search_by="chars")

        else:
            if is_list_of_tuples(search) and replace_by is None:
                search, replace_by = zip(*search)
            search = val_to_list(search)
            replace_by = val_to_list(replace_by)
            if len(replace_by) == 1:
                replace_by = replace_by[0]
            df = df.cols._replace(cols, search, replace_by,
                                  search_by, ignore_case, output_cols)

        return df

    def _replace(self, cols="*", search=None, replace_by=None, search_by="chars", ignore_case=False,
                 output_cols=None) -> DataFrameType:
        """
        Replace a value, list of values by a specified string
        :param cols: '*', list of columns names or a single column name.
        :param search: Values to look at to be replaced
        :param replace_by: New value to replace the old one. Supports an array when searching by characters.
        :param search_by: Can be "full","words","chars" or "values".
        :param ignore_case: Ignore case when searching for match
        :param output_cols:
        :return: DataFrame
        """

        search = val_to_list(search)
        replace_by = one_list_to_val(replace_by)

        if search_by == "full" and (not is_list_of_str(search) or not is_list_of_str(replace_by)):
            search_by = "values"

        if search_by == "chars":
            func = self.F.replace_chars
            func_return_type = str
        elif search_by == "words":
            func = self.F.replace_words
            func_return_type = str
        elif search_by == "full":
            func = self.F.replace_full
            func_return_type = str
        elif search_by == "values":
            func = self.F.replace_values
            func_return_type = None
        else:
            RaiseIt.value_error(
                search_by, ["chars", "words", "full", "values"])

        # Cudf raise and exception if both param are not the same type
        # For example [] ValueError: Cannot convert value of type list  to cudf scalar

        return self.apply(cols, func, args=(search, replace_by), func_return_type=func_return_type,
                          output_cols=output_cols, meta_action=Actions.REPLACE.value, mode="vectorized")

    @staticmethod
    @abstractmethod
    def replace_regex(cols, regex=None, value=None, output_cols=None) -> DataFrameType:
        pass

    def num_to_words(self, cols="*", language="en", output_cols=None) -> DataFrameType:
        w_tokenizer = nltk.tokenize.WhitespaceTokenizer()

        def _num_to_words(text):
            if not is_list_value(text):
                text = w_tokenizer.tokenize(text)
                result = " ".join(
                    [num2words(w, lang=language) if str_to_int(w) else w for w in text])
            else:
                result = [num2words(w, lang=language)
                          if str_to_int(w) else w for w in text]
            return result

        return self.apply(cols, _num_to_words, output_cols=output_cols, mode="map")

    def lemmatize_verbs(self, cols="*", output_cols=None) -> DataFrameType:

        w_tokenizer = nltk.tokenize.WhitespaceTokenizer()
        lemmatizer = nltk.stem.WordNetLemmatizer()

        def lemmatize_text(text):
            return " ".join([lemmatizer.lemmatize(w) for w in w_tokenizer.tokenize(text)])

        return self.apply(cols, lemmatize_text, output_cols=output_cols, mode="map")

    def stem_verbs(self, cols="*", stemmer="porter", language="english", output_cols=None) -> DataFrameType:
        w_tokenizer = nltk.tokenize.WhitespaceTokenizer()

        if stemmer == "snowball":
            stemming = SnowballStemmer(language)
        elif stemmer == "porter":
            stemming = PorterStemmer()
        elif stemmer == "lancaster":
            stemming = LancasterStemmer()

        def stemmer_text(text):
            return " ".join([stemming.stem(w) for w in w_tokenizer.tokenize(text)])

        return self.apply(cols, stemmer_text, output_cols=output_cols, mode="map")

    @staticmethod
    @abstractmethod
    def impute(cols, data_type="continuous", strategy="mean", fill_value=None, output_cols=None) -> DataFrameType:
        pass

    def fill_na(self, cols="*", value=None, output_cols=None) -> DataFrameType:
        """
        Replace null data with a specified value
        :param cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param value: value to replace the nan/None values
        :return:
        """
        df = self.root

        columns = prepare_columns(df, cols, output_cols)

        kw_columns = {}

        for input_col, output_col in columns:
            kw_columns[output_col] = df.data[input_col].fillna(value)
            kw_columns[output_col] = kw_columns[output_col].mask(
                kw_columns[output_col] == "", value)

        return df.cols.assign(kw_columns)

    def count(self) -> int:
        df = self.root
        return len(df.cols.names())

    def unique_values(self, cols="*", values=None, estimate=True, tidy=True, compute=True) -> list:

        df = self.root
        return df.cols.agg_exprs(cols, self.F.unique_values, values, estimate, tidy=tidy, compute=compute)

    def count_uniques(self, cols="*", values=None, estimate=True, tidy=True, compute=True) -> int:
        df = self.root
        return df.cols.agg_exprs(cols, self.F.count_uniques, values, estimate, tidy=tidy, compute=compute)

    def _math(self, cols="*", value=None, operator=None, output_cols=None, output_col=None, name="") -> DataFrameType:
        """
        Helper to process arithmetic operation between columns. If a
        :param cols: Columns to be used to make the calculation
        :param operator: A lambda function
        :return:
        """
        df = self.root
        parsed_cols = parse_columns(df, cols)

        if value is None:
            if not output_col:
                output_col = name+"_"+"_".join(cols)
            expr = reduce(operator, [df[col_name].cols.fill_na(
                "*", 0).cols.to_float() for col_name in parsed_cols])
            return df.cols.assign({output_col: expr})

        else:
            output_cols = get_output_cols(cols, output_cols)
            cols = {}
            for input_col, output_col in zip(parsed_cols, output_cols):
                cols.update({output_col: operator(df[input_col], value)})
            return df.cols.assign(cols)

    def add(self, cols="*", value=None, output_cols=None, output_col=None) -> DataFrameType:
        """
        Add two or more columns
        :param cols: '*', list of columns names or a single column name
        :param output_cols:
        :param output_col: Single output column in case no value is passed
        :return:
        """
        return self._math(cols=cols, value=value, operator=lambda x, y: x + y, output_cols=output_cols, output_col=output_col, name="add")

    def sub(self, cols="*", value=None, output_cols=None, output_col=None) -> DataFrameType:
        """
        Subs two or more columns
        :param cols: '*', list of columns names or a single column name
        :param output_cols:
        :param output_col: Single output column in case no value is passed
        :return:
        """
        return self._math(cols=cols, value=value, operator=lambda x, y: x - y, output_cols=output_cols, output_col=output_col, name="sub")

    def mul(self, cols="*", value=None, output_cols=None, output_col=None) -> DataFrameType:
        """
        Multiply two or more columns
        :param cols: '*', list of columns names or a single column name
        :param output_cols:
        :param output_col: Single output column in case no value is passed
        :return:
        """
        return self._math(cols=cols, value=value, operator=lambda x, y: x * y, output_cols=output_cols, output_col=output_col, name="mul")

    def div(self, cols="*", value=None, output_cols=None, output_col=None) -> DataFrameType:
        """
        Divide two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_cols:
        :param output_col: Single output column in case no value is passed
        :return:
        """
        return self._math(cols=cols, value=value, operator=lambda x, y: x / y, output_cols=output_cols, output_col=output_col, name="div")

    def rdiv(self, cols="*", value=None, output_cols=None, output_col=None) -> DataFrameType:
        """
        Divide two or more columns
        :param columns: '*', list of columns names or a single column name
        :param output_cols:
        :param output_col: Single output column in case no value is passed
        :return:
        """
        return self._math(cols=cols, value=value, operator=lambda x, y: y / x, output_cols=output_cols, output_col=output_col, name="rdiv")

    def z_score(self, cols="*", output_cols=None) -> DataFrameType:
        return self.root.cols.apply(cols, "z_score", func_return_type=float, output_cols=output_cols,
                             meta_action=Actions.Z_SCORE.value, mode="vectorized")

    def modified_z_score(self, cols="*", output_cols=None) -> DataFrameType:
        return self.root.cols.apply(cols, "modified_z_score", func_return_type=float, output_cols=output_cols,
                             meta_action=Actions.Z_SCORE.value, mode="vectorized")

    def standard_scaler(self, cols="*", output_cols=None):
        return self.root.cols.apply(cols, func="standard_scaler", output_cols=output_cols,
                             meta_action=Actions.STANDARD_SCALER.value)

    def max_abs_scaler(self, cols="*", output_cols=None):
        return self.root.cols.apply(cols, func="max_abs_scaler", output_cols=output_cols,
                             meta_action=Actions.MAX_ABS_SCALER.value)

    def min_max_scaler(self, cols="*", output_cols=None):
        return self.root.cols.apply(cols, func="min_max_scaler", output_cols=output_cols,
                             meta_action=Actions.MIN_MAX_SCALER.value)

    def iqr(self, cols="*", more=None, relative_error=RELATIVE_ERROR):
        """
        Return the column Inter Quartile Range
        :param cols:
        :param more: Return info about q1 and q3
        :param relative_error:
        :return:
        """
        df = self.root
        iqr_result = {}
        cols = parse_columns(df, cols)

        quartile = df.cols.percentile(cols, [0.25, 0.5, 0.75], relative_error=relative_error, tidy=False)[
            "percentile"]
        # print("quantile",quartile)
        for col_name in cols:
            if is_null(quartile[col_name]):
                iqr_result[col_name] = np.nan
            else:
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
    def nest(cols, separator="", output_col=None, drop=False, shape="string") -> DataFrameType:
        pass

    def _unnest(self, dfd, input_col, final_columns, separator, splits, mode, output_cols) -> InternalDataFrameType:

        if separator is not None:
            separator = re.escape(separator)

        if mode == "string":
            dfd_new = dfd[input_col].astype(str).str.split(
                separator, expand=True, n=splits - 1)

        elif mode == "array":
            if is_dask_dataframe(dfd):
                def func(value):
                    pdf = value.apply(pd.Series)
                    pdf.columns = final_columns
                    return pdf

                dfd_new = dfd[input_col].map_partitions(
                    func, meta={c: object for c in final_columns})
            else:
                dfd_new = dfd[input_col].apply(pd.Series)

        else:
            RaiseIt.value_error(mode, ["string", "array"])

        return dfd_new

    def unnest(self, cols="*", separator=None, splits=2, index=None, output_cols=None, drop=False, mode="string") -> DataFrameType:
        """
        Split an array or string in different columns
        :param cols: Columns to be un-nested
        :param output_cols: Resulted on or multiple columns after the unnest operation [(output_col_1_1,output_col_1_2),
        (output_col_2_1, output_col_2]
        :param separator: char or regex
        :param splits: Number of columns splits.
        :param index: Return a specific index per columns. [1,2]
        :param drop:
        :param mode:
        """
        df = self.root

        cols = parse_columns(df, cols)

        index = val_to_list(index)
        output_ordered_columns = df.cols.names()

        dfd = df.data

        for idx, input_col in enumerate(cols):

            if is_list_of_tuples(index):
                final_index = index[idx]
            else:
                final_index = index

            if output_cols is None:
                final_columns = [input_col + "_" +
                                 str(i) for i in range(splits)]
            elif is_list_of_tuples(output_cols):
                final_columns = output_cols[idx]
            elif is_list_value(output_cols):
                final_columns = output_cols
            else:
                final_columns = [output_cols + "_" +
                                 str(i) for i in range(splits)]

            dfd_new = self._unnest(
                dfd, input_col, final_columns, separator, splits, mode, output_cols)

            # If columns split is shorter than the number of splits
            new_columns = list(dfd_new.columns)

            if len(final_columns) < len(new_columns):
                dfd_new = dfd_new.drop(
                    columns=new_columns[0:len(final_columns)])
                new_columns = list(dfd_new.columns)

            dfd_new.columns = final_columns[:len(new_columns)]
            df_new = df.new(dfd_new)
            if final_index:
                df_new = df_new.cols.select(final_index[idx])
            df = df.cols.append([df_new])

        df.meta = Meta.action(df.meta, Actions.UNNEST.value, final_columns)

        df = df.cols.move(df_new.cols.names(), "after", cols)

        if drop is True:
            if output_cols is not None:
                columns = [col for col in cols if col not in output_cols]
            else:
                columns = cols
            df = df.cols.drop(columns)

        return df

    @abstractmethod
    def heatmap(self, col_x, col_y, bins_x=10, bins_y=10) -> dict:
        pass

    def hist(self, columns="*", buckets=MAX_BUCKETS, compute=True) -> dict:

        df = self.root
        columns = parse_columns(df, columns)

        @self.F.delayed
        def _bins_col(_columns, _min, _max):
            return {col_name: list(np.linspace(float(_min["min"][col_name]), float(_max["max"][col_name]), num=buckets))
                    for
                    col_name in _columns}

        _min = df.cols.min(columns, compute=True, tidy=False)
        _max = df.cols.max(columns, compute=True, tidy=False)
        _bins = _bins_col(columns, _min, _max)

        @self.F.delayed
        def _hist(pdf, col_name, _bins):
            # import cupy as cp
            _count, bins_edges = np.histogram(pd.to_numeric(
                pdf, errors='coerce'), bins=_bins[col_name])
            # _count, bins_edges = np.histogram(self.to_float(col_name).data[col_name], bins=_bins[col_name])
            # _count, bins_edges = cp.histogram(cp.array(_series.to_gpu_array()), buckets)
            return {col_name: [list(_count), list(bins_edges)]}

        @self.F.delayed
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

        partitions = self.F.to_delayed(df.data)
        c = [_hist(part[col_name], col_name, _bins)
             for part in partitions for col_name in columns]

        d = _agg_hist(c)

        if is_dict(d) or compute is False:
            result = d
        elif compute is True:
            result = d.compute()
        return result

    def quality(self, cols="*", flush=False, compute=True) -> dict:
        """
        :param cols:
        :param infer:
        :param compute:
        Infer the datatype and return the match. mismatch and profiler datatype  for every column.
        In case of date it returns also the format datatype
        :return: {'col_name': {'mismatch': 0, 'missing': 9, 'match': 0, 'inferred_type': 'object'}}
        """

        df = self.root

        # if a dict is passed to cols, assumes it contains the data types
        if is_dict(cols):
            cols_types = cols
        else:
            cols_types = self.root.cols.infer_types(cols)

        result = {}
        profiler_to_mask_func = {
            "decimal": "float"
        }

        quality_props = ["match", "missing", "mismatch"]

        transformed = self._transformed(quality_props)

        for col_name, props in cols_types.items():

            # Gets cached quality
            if col_name not in transformed and not flush:
                cached_props = Meta.get(self.root.meta, f"columns.{col_name}.stats")
                if cached_props and all(prop in cached_props for prop in quality_props):
                    result[col_name] = {"match": cached_props.get("match"),
                                        "missing": cached_props.get("missing"),
                                        "mismatch": cached_props.get("mismatch")}
                    continue

            # Match the profiler dtype with the function. The only function that need to be remapped are decimal and int
            dtype = profiler_to_mask_func.get(
                props["data_type"], props["data_type"])

            matches_mismatches = getattr(df[col_name].mask, dtype)(
                col_name).cols.frequency()

            missing = df.mask.null(col_name).cols.sum()
            values = {list(j.values())[0]: list(j.values())[1] for j in
                      matches_mismatches["frequency"][col_name]["values"]}

            matches = values.get(True)
            mismatches = values.get(False, missing) - missing

            # Ensure that value are not None
            matches = 0 if matches is None else int(matches)
            mismatches = 0 if mismatches is None else int(mismatches)
            missing = 0 if missing is None else int(missing)

            result[col_name] = {"match": matches,
                                "missing": missing, "mismatch": mismatches}

        for col_name in cols_types.keys():
            result[col_name].update({"inferred_type": cols_types[col_name]})

        for col in result:
            self.root.meta = Meta.set(self.root.meta, f"columns.{col}.stats", result[col])

        self._set_transformed_stat(list(result.keys()), ["match", "missing", "mismatch"])

        return result

    @staticmethod
    @abstractmethod
    def count_by_data_types(cols, infer=False, str_funcs=None, int_funcs=None) -> dict:
        pass

    def infer_types(self, cols="*", sample=INFER_PROFILER_ROWS) -> dict:
        """
        Infer datatypes in a dataframe from a sample. First it identify the data type of every value in every cell.
        After that it takes all ghe values apply som heuristic to try to better identify the datatype.
        This function use Pandas no matter the engine you are using.

        :param cols: Columns in which you want to infer the datatype.
        :return:Return a dict with the column and the inferred data type
        """
        df = self.root

        cols = parse_columns(df, cols)

        # Infer the data type from every element in a Series.
        sample_df = df.cols.select(cols).rows.limit(sample).to_optimus_pandas()
        rows_count = sample_df.rows.count()
        sample_dtypes = sample_df.cols.infer_data_types().cols.frequency()

        _unique_counts = sample_df.cols.count_uniques()

        cols_and_inferred_dtype = {}
        for col_name in cols:
            infer_value_counts = sample_dtypes["frequency"][col_name]["values"]
            # Common datatype in a column
            dtype = infer_value_counts[0]["value"]
            second_dtype = infer_value_counts[1]["value"] if len(
                infer_value_counts) > 1 else None

            if dtype == ProfilerDataTypes.MISSING.value and second_dtype:
                _dtype = second_dtype
            elif dtype != ProfilerDataTypes.NULL.value and dtype != ProfilerDataTypes.MISSING.value:
                if dtype == ProfilerDataTypes.INT.value and second_dtype == ProfilerDataTypes.DECIMAL.value:
                    # In case we have integers and decimal values no matter if we have more integer we cast to decimal
                    _dtype = second_dtype
                else:
                    _dtype = dtype
            elif infer_value_counts[0]["count"] < len(sample_dtypes):
                _dtype = second_dtype
            else:
                _dtype = ProfilerDataTypes.OBJECT.value
            _unique_counts = df[col_name].cols.count_uniques()

            if not (any(x in [word.lower() for word in wordninja.split(col_name)] for x in ["zip", "zc"])) \
                    and _dtype == ProfilerDataTypes.ZIP_CODE.value \
                    and _unique_counts / rows_count < ZIPCODE_THRESHOLD:
                _dtype = ProfilerDataTypes.INT.value

            # Is the column categorical?. Try to infer the datatype using the column name
            is_categorical = False

            # if any(x in [word.lower() for word in wordninja.split(col_name)] for x in ["id", "type"]):
            #     is_categorical = False

            if _dtype in PROFILER_CATEGORICAL_DTYPES \
                    or _unique_counts / rows_count < CATEGORICAL_THRESHOLD \
                    or any(x in [word.lower() for word in wordninja.split(col_name)] for x in ["id", "type"]):
                is_categorical = True

            cols_and_inferred_dtype[col_name] = {
                "data_type": _dtype, "categorical": is_categorical}
            if dtype == ProfilerDataTypes.DATETIME.value:
                # pydatainfer do not accepts None value so we must filter them
                filtered_dates = [i for i in sample_df[col_name].to_list() if i]
                cols_and_inferred_dtype[col_name].update(
                    {"format": pydateinfer.infer_dtypes(filtered_dates)})

        for col in cols_and_inferred_dtype:
            self.root.meta = Meta.set(self.root.meta, f"columns.{col}.stats.inferred_type", cols_and_inferred_dtype[col])

        return cols_and_inferred_dtype

    def frequency(self, cols="*", n=MAX_BUCKETS, percentage=False, total_rows=None, count_uniques=False,
                  compute=True, tidy=False) -> dict:
        df = self.root
        cols = parse_columns(df, cols)

        @self.F.delayed
        def series_to_dict(_series, _total_freq_count=None):
            _result = [{"value": i, "count": j}
                       for i, j in self._series_to_dict_delayed(_series).items()]

            if _total_freq_count is None:
                _result = {_series.name: {"values": _result}}
            else:
                _result = {_series.name: {"values": _result,
                                          "count_uniques": int(_total_freq_count)}}

            return _result

        @self.F.delayed
        def flat_dict(top_n):
            return {"frequency": {key: value for ele in top_n for key, value in ele.items()}}

        @self.F.delayed
        def freq_percentage(_value_counts, _total_rows):

            for i, j in _value_counts.items():
                for x in list(j.values())[0]:
                    x["percentage"] = round(
                        (x["count"] * 100 / _total_rows), 2)

            return _value_counts

        value_counts = [df.data[col_name].value_counts() for col_name in cols]
        n_largest = [_value_counts.nlargest(n)
                     for _value_counts in value_counts]

        if count_uniques is True:
            count_uniques = [_value_counts.count()
                             for _value_counts in value_counts]
            b = [series_to_dict(_n_largest, _count)
                 for _n_largest, _count in zip(n_largest, count_uniques)]
        else:
            b = [series_to_dict(_n_largest) for _n_largest in n_largest]

        c = flat_dict(b)

        if percentage:
            c = freq_percentage(c, df.delayed(len)(df))

        if compute is True:
            result = dd.compute(c)[0]
        else:
            result = c

        if tidy is True:
            result = result["frequency"]

        return result

    def boxplot(self, cols) -> dict:
        """
        Output the boxplot in python dict format
        :param cols: Columns to be processed
        :return:
        """
        df = self.root
        cols = parse_columns(df, cols)
        stats = {}

        for col_name in cols:
            iqr = df.cols.iqr(col_name, more=True)
            lb = iqr["q1"] - (iqr["iqr"] * 1.5)
            ub = iqr["q3"] + (iqr["iqr"] * 1.5)

            _mean = df.cols.mean(cols)

            query = ((df[col_name] < lb) | (df[col_name] > ub))
            # Fliers are outliers points
            fliers = df.rows.select(query).cols.select(
                col_name).rows.limit(1000).to_dict()
            stats[col_name] = {'mean': _mean, 'median': iqr["q2"], 'q1': iqr["q1"], 'q3': iqr["q3"], 'whisker_low': lb,
                               'whisker_high': ub,
                               'fliers': [fliers[0][col_name]], 'label': one_list_to_val(col_name)}

        return stats

    def names(self, col_names="*", by_data_types=None, invert=False, is_regex=False) -> list:

        cols = parse_columns(self.root, col_names, filter_by_column_types=by_data_types, invert=invert,
                             is_regex=is_regex)
        return cols

    def count_zeros(self, cols="*", tidy=True, compute=True):
        return self.count_equal(cols, 0, tidy)
        # df = self.root
        # return df.cols.agg_exprs(cols, self.F.count_zeros, tidy=tidy, compute=compute)

    def qcut(self, cols="*", quantiles=None, output_cols=None):

        return self.apply(cols, self.F.qcut, args=quantiles, output_cols=output_cols, meta_action=Actions.ABS.value,
                          mode="vectorized")

    def cut(self, cols="*", bins=None, labels=None, default=None, output_cols=None) -> DataFrameType:

        return self.apply(cols, self.F.cut, output_cols=output_cols, args=(bins, labels, default),
                          meta_action=Actions.CUT.value,
                          mode="vectorized")

    def clip(self, cols="*", lower_bound=None, upper_bound=None, output_cols=None) -> DataFrameType:

        def _clip(value):
            return self.F.clip(value, lower_bound, upper_bound)

        return self.apply(cols, _clip, output_cols=output_cols, meta_action=Actions.CLIP.value, mode="vectorized")

    @staticmethod
    @abstractmethod
    def string_to_index(cols=None, output_cols=None) -> DataFrameType:
        pass

    @staticmethod
    @abstractmethod
    def index_to_string(cols=None, output_cols=None) -> DataFrameType:
        pass

    # URL methods

    def domain(self, cols="*", output_cols=None) -> DataFrameType:
        """
        From https://www.hi-bumblebee.com it returns hi-bumblebee.com
        :param cols:
        :param output_cols:
        :return:
        """

        df = self.root
        return df.cols.apply(cols, self.F.domain, output_cols=output_cols, meta_action=Actions.DOMAIN.value,
                             mode="map")

    def top_domain(self, cols="*", output_cols=None) -> DataFrameType:
        """
        From https://www.hi-bumblebee.com it returns hi-bumblebee.com
        :param cols:
        :param output_cols:
        :return:
        """

        df = self.root
        return df.cols.apply(cols, self.F.top_domain, output_cols=output_cols, meta_action=Actions.TOP_DOMAIN.value,
                             mode="map")

    def sub_domain(self, cols="*", output_cols=None) -> DataFrameType:
        # From https://www.hi-bumblebee.com:8080 it returns www

        df = self.root
        return df.cols.apply(cols, self.F.sub_domain, output_cols=output_cols, meta_action=Actions.SUB_DOMAIN.value,
                             mode="map")

    def url_scheme(self, cols="*", output_cols=None) -> DataFrameType:
        # From https://www.hi-bumblebee.com it returns https
        df = self.root
        return df.cols.apply(cols, self.F.url_scheme, output_cols=output_cols,
                             meta_action=Actions.URL_SCHEME.value,
                             mode="map")

    def url_path(self, cols="*", output_cols=None) -> DataFrameType:

        df = self.root
        return df.cols.apply(cols, self.F.url_path, output_cols=output_cols,
                             meta_action=Actions.URL_PATH.value,
                             mode="map")

    def url_file(self, cols="*", output_cols=None) -> DataFrameType:

        df = self.root
        return df.cols.apply(cols, self.F.url_file, output_cols=output_cols,
                             meta_action=Actions.URL_FILE.value,
                             mode="map")

    def url_query(self, cols="*", output_cols=None) -> DataFrameType:

        df = self.root
        return df.cols.apply(cols, self.F.url_query, output_cols=output_cols, meta_action=Actions.URL_QUERY.value,
                             mode="map")

    def url_fragment(self, cols="*", output_cols=None) -> DataFrameType:

        df = self.root
        return df.cols.apply(cols, self.F.url_fragment, output_cols=output_cols, meta_action=Actions.URL_FRAGMENT.value,
                             mode="map")

    def host(self, cols="*", output_cols=None) -> DataFrameType:

        df = self.root
        return df.cols.apply(cols, self.F.host, output_cols=output_cols, meta_action=Actions.HOST.value,
                             mode="map")

    def port(self, cols="*", output_cols=None) -> DataFrameType:

        df = self.root
        return df.cols.apply(cols, self.F.port, output_cols=output_cols, meta_action=Actions.PORT.value,
                             mode="map")

    # Email functions
    def email_username(self, cols="*", output_cols=None) -> DataFrameType:

        df = self.root
        return df.cols.apply(cols, self.F.email_username, output_cols=output_cols,
                             meta_action=Actions.EMAIL_USER.value,
                             mode="vectorized")

    def email_domain(self, cols="*", output_cols=None) -> DataFrameType:
        df = self.root
        return df.cols.apply(cols, self.F.email_domain, output_cols=output_cols,
                             meta_action=Actions.EMAIL_DOMAIN.value,
                             mode="vectorized")

    # Mask functions

    def _mask(self, cols="*", method: str = None, output_cols=None, rename_func=True, *args, **kwargs) -> DataFrameType:

        append_df = getattr(self.root.mask, method)(cols=cols, *args, **kwargs)

        if output_cols:
            append_df = append_df.cols.rename(cols, output_cols)
        elif rename_func:
            if rename_func == True:
                def rename_func(n): return f"{n}_{method}"
            append_df = append_df.cols.rename(rename_func)

        return self.append(append_df)

    def _any_mask(self, cols="*", method: str = None, inverse=False, tidy=True, compute=True, *args, **kwargs) -> bool:

        mask = getattr(self.root.mask, method)(cols=cols, *args, **kwargs)

        if inverse:
            # assigns True if there is any False value
            values = {col: not self.F.all(mask.data[col])
                      for col in mask.cols.names()}
        else:
            # assigns True if there is any True value
            values = {col: self.F.any(mask.data[col])
                      for col in mask.cols.names()}

        if compute:
            return self.F.compute(format_dict(values, tidy)) 
        else:
            return format_dict(values, tidy)

    def _count_mask(self, cols="*", method: str = None, inverse=False, tidy=True, compute=True, *args, **kwargs) -> bool:

        mask = getattr(self.root.mask, method)(cols=cols, *args, **kwargs)

        if inverse:
            # assigns True if there is any False value
            values = {col: not mask.data[col].sum()
                      for col in mask.cols.names()}
        else:
            # assigns True if there is any True value
            values = {col: mask.data[col].sum() for col in mask.cols.names()}

        if compute:
            return self.F.compute(format_dict(values, tidy)) 
        else:
            return format_dict(values, tidy)

    # Any mask

    def any_greater_than(self, cols="*", value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "greater_than", value=value, inverse=inverse, tidy=tidy, compute=compute)

    def any_greater_than_equal(self, cols="*", value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "greater_than_equal", value=value, inverse=inverse, tidy=tidy, compute=compute)

    def any_less_than(self, cols="*", value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "less_than", value=value, inverse=inverse, tidy=tidy, compute=compute)

    def any_less_than_equal(self, cols="*", value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "less_than_equal", value=value, inverse=inverse, tidy=tidy, compute=compute)

    def any_between(self, cols="*", lower_bound=None, upper_bound=None, equal=True, bounds=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "between", lower_bound=lower_bound, upper_bound=upper_bound, equal=equal, bounds=bounds, inverse=inverse, tidy=tidy, compute=compute)

    def any_equal(self, cols="*", value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "equal", value=value, inverse=inverse, tidy=tidy, compute=compute)

    def any_not_equal(self, cols="*", value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "not_equal", value=value, inverse=inverse, tidy=tidy, compute=compute)

    def any_missing(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "missing", inverse=inverse, tidy=tidy, compute=compute)

    def any_null(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "null", inverse=inverse, tidy=tidy, compute=compute)

    def any_none(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "none", inverse=inverse, tidy=tidy, compute=compute)

    def any_nan(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "nan", inverse=inverse, tidy=tidy, compute=compute)

    def any_empty(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "empty", inverse=inverse, tidy=tidy, compute=compute)

    def any_mismatch(self, cols="*", data_type=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "mismatch", data_type=data_type, inverse=inverse, tidy=tidy, compute=compute)

    def any_duplicated(self, cols="*", keep="first", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "duplicated", keep=keep, inverse=inverse, tidy=tidy, compute=compute)

    # def any_uniques(self, cols="*", keep="first", inverse=False, tidy=True, compute=True):
    #     return self._any_mask(cols, "unique", keep=keep, inverse=inverse, tidy=tidy, compute=compute)

    def any_match(self, cols="*", regex=None, data_type=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "match", regex=regex, data_type=data_type, inverse=inverse, tidy=tidy, compute=compute)

    def any_match_data_type(self, cols="*", data_type=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "match_data_type", data_type=data_type, inverse=inverse, tidy=tidy, compute=compute)

    def any_match_regex(self, cols="*", regex=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "match_regex", regex=regex, inverse=inverse, tidy=tidy, compute=compute)

    def any_starting_with(self, cols="*", value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "starts_with", value=value, inverse=inverse, tidy=tidy, compute=compute)

    def any_ending_with(self, cols="*", value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "ends_with", value=value, inverse=inverse, tidy=tidy, compute=compute)

    def any_containing(self, cols="*", value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "contains", value=value, inverse=inverse, tidy=tidy, compute=compute)

    def any_value_in(self, cols="*", values=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "value_in", values=values, inverse=inverse, tidy=tidy, compute=compute)

    def any_match_pattern(self, cols="*", pattern=None, inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "match_pattern", pattern=pattern, inverse=inverse, tidy=tidy, compute=compute)

    def any_expression(self, value=None, inverse=False, tidy=True, compute=True):
        return self._any_mask("*", "expression", value=value, inverse=inverse, tidy=tidy, compute=compute)

    # Any mask (type)

    def any_str(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "str", inverse=inverse, tidy=tidy, compute=compute)

    def any_int(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "int", inverse=inverse, tidy=tidy, compute=compute)

    def any_float(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "float", inverse=inverse, tidy=tidy, compute=compute)

    def any_numeric(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "numeric", inverse=inverse, tidy=tidy, compute=compute)

    def any_email(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "email", inverse=inverse, tidy=tidy, compute=compute)

    def any_ip(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "ip", inverse=inverse, tidy=tidy, compute=compute)

    def any_url(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "url", inverse=inverse, tidy=tidy, compute=compute)

    def any_gender(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "gender", inverse=inverse, tidy=tidy, compute=compute)

    def any_boolean(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "boolean", inverse=inverse, tidy=tidy, compute=compute)

    def any_zip_code(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "zip_code", inverse=inverse, tidy=tidy, compute=compute)

    def any_credit_card_number(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "credit_card_number", inverse=inverse, tidy=tidy, compute=compute)

    def any_datetime(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "datetime", inverse=inverse, tidy=tidy, compute=compute)

    def any_object(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "object", inverse=inverse, tidy=tidy, compute=compute)

    def any_array(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "array", inverse=inverse, tidy=tidy, compute=compute)

    def any_phone_number(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "phone_number", inverse=inverse, tidy=tidy, compute=compute)

    def any_social_security_number(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "social_security_number", inverse=inverse, tidy=tidy, compute=compute)

    def any_http_code(self, cols="*", inverse=False, tidy=True, compute=True):
        return self._any_mask(cols, "http_code", inverse=inverse, tidy=tidy, compute=compute)

    # Count mask

    def count_greater_than(self, cols="*", value=None, tidy=True, compute=True):
        return self._count_mask(cols, "greater_than", value=value, tidy=tidy, compute=compute)

    def count_greater_than_equal(self, cols="*", value=None, tidy=True, compute=True):
        return self._count_mask(cols, "greater_than_equal", value=value, tidy=tidy, compute=compute)

    def count_less_than(self, cols="*", value=None, tidy=True, compute=True):
        return self._count_mask(cols, "less_than", value=value, tidy=tidy, compute=compute)

    def count_less_than_equal(self, cols="*", value=None, tidy=True, compute=True):
        return self._count_mask(cols, "less_than_equal", value=value, tidy=tidy, compute=compute)

    def count_between(self, cols="*", lower_bound=None, upper_bound=None, equal=True, bounds=None, tidy=True, compute=True):
        return self._count_mask(cols, "between", lower_bound=lower_bound, upper_bound=upper_bound, equal=equal, bounds=bounds, tidy=tidy, compute=compute)

    def count_equal(self, cols="*", value=None, tidy=True, compute=True):
        return self._count_mask(cols, "equal", value=value, tidy=tidy, compute=compute)

    def count_not_equal(self, cols="*", value=None, tidy=True, compute=True):
        return self._count_mask(cols, "not_equal", value=value, tidy=tidy, compute=compute)

    def count_missings(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "missing", tidy=tidy, compute=compute)

    def count_nulls(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "null", tidy=tidy, compute=compute)

    def count_none(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "none", tidy=tidy, compute=compute)

    def count_nan(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "nan", tidy=tidy, compute=compute)

    def count_empty(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "empty", tidy=tidy, compute=compute)

    def count_mismatch(self, cols="*", data_type=None, tidy=True, compute=True):
        return self._count_mask(cols, "mismatch", data_type=data_type, tidy=tidy, compute=compute)

    def count_duplicated(self, cols="*", keep="first", tidy=True, compute=True):
        return self._count_mask(cols, "duplicated", keep=keep, tidy=tidy, compute=compute)

    # def count_uniques(self, cols="*", keep="first", tidy=True, compute=True):
    #     return self._count_mask(cols, "unique", keep=keep, tidy=tidy, compute=compute)

    def count_match(self, cols="*", regex=None, data_type=None, tidy=True, compute=True):
        return self._count_mask(cols, "match", regex=regex, data_type=data_type, tidy=tidy, compute=compute)

    def count_data_type(self, cols="*", data_type=None, tidy=True, compute=True):
        return self._count_mask(cols, "match_data_type", data_type=data_type, tidy=tidy, compute=compute)

    def count_regex(self, cols="*", regex=None, tidy=True, compute=True):
        return self._count_mask(cols, "match_regex", regex=regex, tidy=tidy, compute=compute)

    def count_starting_with(self, cols="*", value=None, tidy=True, compute=True):
        return self._count_mask(cols, "starts_with", value=value, tidy=tidy, compute=compute)

    def count_ending_with(self, cols="*", value=None, tidy=True, compute=True):
        return self._count_mask(cols, "ends_with", value=value, tidy=tidy, compute=compute)

    def count_containing(self, cols="*", value=None, tidy=True, compute=True):
        return self._count_mask(cols, "contains", value=value, tidy=tidy, compute=compute)

    def count_values_in(self, cols="*", values=None, tidy=True, compute=True):
        return self._count_mask(cols, "value_in", values=values, tidy=tidy, compute=compute)

    def count_match_pattern(self, cols="*", pattern=None, tidy=True, compute=True):
        return self._count_mask(cols, "match_pattern", pattern=pattern, tidy=tidy, compute=compute)

    def count_expression(self, value=None, inverse=False, tidy=True, compute=True):
        return self._count_mask("*", "expression", value=value, inverse=inverse, tidy=tidy, compute=compute)

    # Count mask (data types)

    def count_str(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "str", tidy=tidy, compute=compute)

    def count_int(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "int", tidy=tidy, compute=compute)

    def count_float(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "float", tidy=tidy, compute=compute)

    def count_numeric(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "numeric", tidy=tidy, compute=compute)

    def count_email(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "email", tidy=tidy, compute=compute)

    def count_ip(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "ip", tidy=tidy, compute=compute)

    def count_url(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "url", tidy=tidy, compute=compute)

    def count_gender(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "gender", tidy=tidy, compute=compute)

    def count_boolean(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "boolean", tidy=tidy, compute=compute)

    def count_zip_code(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "zip_code", tidy=tidy, compute=compute)

    def count_credit_card_number(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "credit_card_number", tidy=tidy, compute=compute)

    def count_datetime(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "datetime", tidy=tidy, compute=compute)

    def count_object(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "object", tidy=tidy, compute=compute)

    def count_array(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "array", tidy=tidy, compute=compute)

    def count_phone_number(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "phone_number", tidy=tidy, compute=compute)

    def count_social_security_number(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "social_security_number", tidy=tidy, compute=compute)

    def count_http_code(self, cols="*", tidy=True, compute=True):
        return self._count_mask(cols, "http_code", tidy=tidy, compute=compute)

    # Append mask

    def greater_than(self, cols="*", value=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_greater_than_{value}"
        return self._mask(cols, "greater_than", output_cols, rename_func, value=value)

    def greater_than_equal(self, cols="*", value=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_greater_than_equal_{value}"
        return self._mask(cols, "greater_than_equal", output_cols, rename_func, value=value)

    def less_than(self, cols="*", value=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_less_than_{value}"
        return self._mask(cols, "less_than", output_cols, rename_func, value=value)

    def less_than_equal(self, cols="*", value=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_less_than_equal_{value}"
        return self._mask(cols, "less_than_equal", output_cols, rename_func, value=value)

    def between(self, cols="*", lower_bound=None, upper_bound=None, equal=True, bounds=None, output_cols=None, drop=True) -> DataFrameType:
        value = str(bounds) if bounds else str((lower_bound, upper_bound))
        rename_func = False if drop else lambda n: f"{n}_between_{value}"
        return self._mask(cols, "between", output_cols, rename_func, lower_bound=lower_bound, upper_bound=upper_bound, equal=equal, bounds=bounds)

    def equal(self, cols="*", value=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_equal_{value}"
        return self._mask(cols, "equal", output_cols, rename_func, value=value)

    def not_equal(self, cols="*", value=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_not_equal_{value}"
        return self._mask(cols, "not_equal", output_cols, rename_func, value=value)

    def missing(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "missing", output_cols, rename_func=not drop)

    def null(self, cols="*", how="all", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "null", output_cols, rename_func=not drop, how=how)

    def none(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "none", output_cols, rename_func=not drop)

    def nan(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "nan", output_cols, rename_func=not drop)

    def empty(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "empty", output_cols, rename_func=not drop)

    def mismatch(self, cols="*", data_type=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_mismatch_{data_type}"
        return self._mask(cols, "mismatch", output_cols, rename_func, data_type=data_type)

    def duplicated(self, cols="*", keep="first", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "duplicated", output_cols, rename_func=not drop, keep=keep)

    def unique(self, cols="*", keep="first", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "unique", output_cols, rename_func=not drop, keep=keep)

    def match(self, cols="*", regex=None, data_type=None, output_cols=None, drop=True) -> DataFrameType:
        if data_type is None:
            return self.match_regex(cols=cols, regex=regex, output_cols=output_cols, drop=drop)
        else:
            return self.match_data_type(cols=cols, data_type=data_type, output_cols=output_cols, drop=drop)

    def match_regex(self, cols="*", regex=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_match_{regex}"
        return self._mask(cols, "match_regex", output_cols, rename_func, regex=regex)

    def match_data_type(self, cols="*", data_type=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_match_{data_type}"
        return self._mask(cols, "match_data_type", output_cols, rename_func, data_type=data_type)

    def match_pattern(self, cols="*", pattern=None, output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "pattern", output_cols, rename_func=not drop, pattern=pattern)

    def starts_with(self, cols="*", value=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_starts_with_{value}"
        return self._mask(cols, "starts_with", output_cols, rename_func, value=value)

    def ends_with(self, cols="*", value=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_ends_with_{value}"
        return self._mask(cols, "ends_with", output_cols, rename_func, value=value)

    def contains(self, cols="*", value=None, output_cols=None, drop=True) -> DataFrameType:
        rename_func = False if drop else lambda n: f"{n}_contains_{value}"
        return self._mask(cols, "contains", output_cols, rename_func, value=value)

    def value_in(self, cols="*", values=None, output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "value_in", output_cols, rename_func=not drop, values=values)

    def expression(self, where=None, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "expression", output_cols, rename_func=not drop, where=where)

    # Append mask (types)

    def str_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "str", output_cols, rename_func=not drop)

    def int_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "int", output_cols, rename_func=not drop)

    def float_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "float", output_cols, rename_func=not drop)

    def numeric_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "numeric", output_cols, rename_func=not drop)

    def email_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "email", output_cols, rename_func=not drop)

    def ip_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "ip", output_cols, rename_func=not drop)

    def url_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "url", output_cols, rename_func=not drop)

    def gender_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "gender", output_cols, rename_func=not drop)

    def boolean_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "boolean", output_cols, rename_func=not drop)

    def zip_code_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "zip_code", output_cols, rename_func=not drop)

    def credit_card_number_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "credit_card_number", output_cols, rename_func=not drop)

    def datetime_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "datetime", output_cols, rename_func=not drop)

    def object_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "object", output_cols, rename_func=not drop)

    def array_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "array", output_cols, rename_func=not drop)

    def phone_number_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "phone_number", output_cols, rename_func=not drop)

    def social_security_number_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "social_security_number", output_cols, rename_func=not drop)

    def http_code_values(self, cols="*", output_cols=None, drop=True) -> DataFrameType:
        return self._mask(cols, "http_code", output_cols, rename_func=not drop)

    # String clustering algorithms

    def fingerprint(self, cols="*", output_cols=None) -> DataFrameType:
        """
        Create the fingerprint for a column
        :param df: Dataframe to be processed
        :param cols: Column to be processed
        :return:
        """

        df = self.root

        # https://github.com/OpenRefine/OpenRefine/blob/master/main/src/com/google/refine/clustering/binning/FingerprintKeyer.java#L56
        def _split_sort_remove_join(value):
            """
            Helper function to split, remove duplicates, sort and join back together
            """
            # Split into whitespace-separated token
            # print("value", type(value), value)
            split_key = value.split()

            # Sort and remove duplicated items
            split_key = sorted(set(split_key))

            # join the tokens back together
            return " ".join(split_key)

        cols = parse_columns(df, cols)
        output_cols = get_output_cols(cols, output_cols)

        for input_col, output_col in zip(cols, output_cols):
            df = (df
                  .cols.trim(input_col, output_col)
                  .cols.lower(output_col)
                  .cols.remove_special_chars(output_col)
                  .cols.normalize_chars(output_col)
                  .cols.apply(output_col, _split_sort_remove_join, "string", mode="map")
                  )

        df.meta = Meta.action(df.meta, Actions.FINGERPRINT.value, output_cols)

        return df

    def pos(self, cols="*", output_cols=None) -> DataFrameType:
        df = self.root

        cols = parse_columns(df, cols)
        output_cols = get_output_cols(cols, output_cols)

        w_tokenizer = nltk.tokenize.WhitespaceTokenizer()

        def calculate_ngrams(text):
            if not is_list_value(text):
                text = w_tokenizer.tokenize(text)
            return nltk.pos_tag(text)

        for input_col, output_col in zip(cols, output_cols):
            df = df.cols.apply(input_col, calculate_ngrams,
                               "string", output_cols=output_col, mode="map")
        return df

    def ngrams(self, cols="*", n_size=2, output_cols=None) -> DataFrameType:
        """
            Calculate the ngram for a fingerprinted string
            :param df: Dataframe to be processed
            :param cols: Columns to be processed
            :param n_size:
            :return:
            """

        df = self.root

        cols = parse_columns(df, cols)
        output_cols = get_output_cols(cols, output_cols)

        def calculate_ngrams(value):
            return list(map("".join, list(ngrams(value, n_size))))

        for input_col, output_col in zip(cols, output_cols):
            df = df.cols.apply(output_col, calculate_ngrams,
                               "string", output_cols=output_col, mode="map")

        df.meta = Meta.action(df.meta, Actions.NGRAMS.value, output_cols)

        return df

    def ngram_fingerprint(self, cols="*", n_size=2, output_cols=None) -> DataFrameType:
        """
        Calculate the ngram for a fingerprinted string
        :param df: Dataframe to be processed
        :param cols: Columns to be processed
        :param n_size:
        :return:
        """

        df = self.root
        from nltk import ngrams

        def calculate_ngrams(value):
            ngram = list(map("".join, list(ngrams(value, n_size))))
            ngram = sorted(set(ngram))
            _result = "".join(ngram)

            return _result

        cols = parse_columns(df, cols)
        output_cols = get_output_cols(cols, output_cols)

        for input_col, output_col in zip(cols, output_cols):
            df = (df
                  .cols.copy(input_col, output_col)
                  .cols.lower(output_col)
                  .cols.remove_white_spaces(output_col)
                  .cols.remove_special_chars(output_col)
                  .cols.normalize_chars(output_col)
                  .cols.apply(output_col, calculate_ngrams, "string", output_cols=output_col, mode="map")
                  )

        df.meta = Meta.action(
            df.meta, Actions.NGRAM_FINGERPRINT.value, output_cols)

        return df

    def metaphone(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, jellyfish.metaphone, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.METAPHONE.value, mode="map", func_type="column_expr")

    def double_metaphone(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, doublemetaphone, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.DOUBLE_METAPHONE.value, mode="map", func_type="column_expr")

    def levenshtein(self, col_A, col_B, output_cols=None):
        return self.apply(None, self.F.levenshtein, args=(col_A, col_B,), func_return_type=str,
                          output_cols=output_cols,
                          meta_action=Actions.METAPHONE.value, mode="map", func_type="column_expr")

    def nysiis(self, cols="*", output_cols=None) -> DataFrameType:
        """
        NYSIIS (New York State Identification and Intelligence System)
        :param cols:
        :param output_cols:
        :return:
        """
        return self.apply(cols, jellyfish.nysiis, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.NYSIIS.value, mode="map", func_type="column_expr")

    def match_rating_codex(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, jellyfish.match_rating_codex, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.MATCH_RATING_CODEX.value, mode="map", func_type="column_expr")

    def double_methaphone(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, jellyfish.dou, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.DOUBLE_METAPHONE.value, mode="map", func_type="column_expr")

    def soundex(self, cols="*", output_cols=None) -> DataFrameType:
        return self.apply(cols, jellyfish.soundex, func_return_type=str, output_cols=output_cols,
                          meta_action=Actions.SOUNDEX.value, mode="map", func_type="column_expr")

    def tf_idf(self, features) -> DataFrameType:

        df = self.root
        vectorizer = TfidfVectorizer()
        X = df[features]._to_values().ravel()
        vectors = vectorizer.fit_transform(X)

        feature_names = vectorizer.get_feature_names()
        dense = vectors.todense()
        denselist = dense.tolist()
        return self.root.new(pd.DataFrame(denselist, columns=feature_names))

    def bag_of_words(self, features, analyzer="word", ngram_range=2) -> DataFrameType:
        """

        :param features:
        :param ngram_range:
        :return:
        """

        df = self.root
        if is_int(ngram_range):
            ngram_range = (ngram_range, ngram_range)

        features = parse_columns(df, features)

        df = df.cols.select(features).rows.drop_na()

        X = df[features]._to_values().ravel()
        vectorizer = CountVectorizer(
            ngram_range=ngram_range, analyzer=analyzer)
        matrix = vectorizer.fit_transform(X)

        return self.root.new(pd.DataFrame(matrix.toarray(), columns=vectorizer.get_feature_names()))
