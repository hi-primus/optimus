import string
from abc import abstractmethod, ABC
from enum import Enum

import numpy as np
from glom import glom

from optimus.helpers.columns import parse_columns, check_column_numbers, prepare_columns
from optimus.helpers.constants import RELATIVE_ERROR, ProfilerDataTypes, Actions
from optimus.helpers.converter import format_dict
# This implementation works for Spark, Dask, dask_cudf
from optimus.helpers.core import val_to_list
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_dict


class BaseColumns(ABC):
    """Base class for all Cols implementations"""

    def __init__(self, df):
        self.df = df

    @staticmethod
    @abstractmethod
    def append(*args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def select(columns="*", regex=None, data_type=None, invert=False, accepts_missing_cols=False) -> str:
        pass

    @staticmethod
    @abstractmethod
    def copy(input_cols, output_cols=None, columns=None):
        pass

    @staticmethod
    @abstractmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def apply_expr(input_cols, func=None, args=None, filter_col_by_dtypes=None, output_cols=None,
                   meta=None):
        pass

    @staticmethod
    @abstractmethod
    def apply(input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
              filter_col_by_dtypes=None, output_cols=None, skip_output_cols_processing=False,
              meta="apply"):
        pass

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
    def slice(input_cols, output_cols, start, stop, step):
        pass

    @staticmethod
    @abstractmethod
    def extract(input_cols, output_cols, regex):
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

    def profiler_dtypes(self, columns):
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
        :param decimals:
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

    def move(self, column, position, ref_col=None):
        """
        Move a column to specific position
        :param column: Column to be moved
        :param position: Column new position. Accepts 'after', 'before', 'beginning', 'end'
        :param ref_col: Column taken as reference
        :return: Spark DataFrame
        """

        df = self.df
        # Check that column is a string or a list
        column = parse_columns(df, column)
        ref_col = parse_columns(df, ref_col)

        # Get dataframe columns
        columns = df.cols.names()

        # Get source and reference column index position
        new_index = columns.index(ref_col[0])

        # Column to move
        column_to_move_index = columns.index(column[0])

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
            new_index = len(columns)
        else:
            RaiseIt.value_error(position, ["after", "before", "beginning", "end"])

        # Move the column to the new place
        columns.insert(new_index, columns.pop(column_to_move_index))  # insert and delete a element

        return df[columns]

    @staticmethod
    @abstractmethod
    def keep(columns=None, regex=None):
        pass

    def sort(self, columns=None, order: [str, list] = "asc"):
        """
        Sort data frames columns asc or desc
        :param order: 'asc' or 'desc' accepted
        :param columns:
        :return: Spark DataFrame
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

    @staticmethod
    @abstractmethod
    def drop(columns=None, regex=None, data_type=None):
        pass

    def dtypes(self, columns="*"):
        """
        Return the column(s) data type as string
        :param columns: Columns to be processed
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        data_types = ({k: str(v) for k, v in dict(df.dtypes).items()})
        return format_dict({col_name: data_types[col_name] for col_name in columns})

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

    @staticmethod
    @abstractmethod
    def create_exprs(columns, funcs, *args):
        pass

    def agg_exprs(self, columns, funcs, *args):
        """
        Create and run aggregation
        :param columns:
        :param funcs:
        :param args:
        :return:
        """
        return self.exec_agg(self.create_exprs(columns, funcs, *args))

    @staticmethod
    @abstractmethod
    def exec_agg(exprs):
        pass

    def min(self, columns):
        df = self.df
        return self.agg_exprs(columns, df.functions.min)

    def max(self, columns):
        df = self.df
        return self.agg_exprs(columns, df.functions.max)

    def range(self, columns):
        df = self.df
        return self.agg_exprs(columns, df.functions.range_agg)

    def percentile(self, columns, values=None, relative_error=RELATIVE_ERROR):
        df = self.df
        # values = [str(v) for v in values]
        if values is None:
            values = [0.5]
        return self.agg_exprs(columns, df.functions.percentile_agg, df, values, relative_error)

    def median(self, columns, relative_error=RELATIVE_ERROR):
        return format_dict(self.percentile(columns, [0.5], relative_error))

    # Descriptive Analytics
    # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/

    def mad(self, columns, relative_error=RELATIVE_ERROR, more=None):
        df = self.df
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        result = {}
        funcs = [df.functions.mad_agg]

        return self.agg_exprs(columns, funcs, more)

    def std(self, columns):
        df = self.df
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")
        return self.agg_exprs(columns, df.functions.stddev)

    def kurt(self, columns):
        df = self.df

        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return self.agg_exprs(columns, df.functions.kurtosis)

    def mean(self, columns):
        df = self.df
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return self.agg_exprs(columns, df.functions.mean)

    def skewness(self, columns):
        df = self.df
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return self.agg_exprs(columns, df.functions.skewness)

    def sum(self, columns):
        df = self.df
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(self.agg_exprs(columns, df.functions.sum))

    def variance(self, columns):
        df = self.df
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(self.agg_exprs(columns, df.functions.variance))

    @staticmethod
    @abstractmethod
    def abs(columns):
        pass

    @staticmethod
    @abstractmethod
    def mode(columns):
        pass

    @staticmethod
    @abstractmethod
    def lower(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def upper(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def trim(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def reverse(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def remove(columns, search=None, search_by="chars", output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def remove_accents(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def remove_special_chars(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def remove_white_spaces(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def year(self, input_cols, output_cols=None):
        pass
    @staticmethod
    @abstractmethod
    def month(self, input_cols, output_cols=None):
        pass
    @staticmethod
    @abstractmethod
    def day(self, input_cols, output_cols=None):
        pass
    @staticmethod
    @abstractmethod
    def hour(self, input_cols, output_cols=None):
        pass
    @staticmethod
    @abstractmethod
    def minute(self, input_cols, output_cols=None):
        pass
    @staticmethod
    @abstractmethod
    def second(self, input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def date_transform(input_cols, current_format=None, output_format=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def years_between(input_cols, date_format=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def replace(input_cols, search=None, replace_by=None, search_by="chars", output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def replace_regex(input_cols, regex=None, value=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def impute(input_cols, data_type="continuous", strategy="mean", output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def fill_na(input_cols, value=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def is_na(input_cols, output_cols=None):
        pass

    @staticmethod
    def count(self):
        pass

    @staticmethod
    @abstractmethod
    def count_na(columns):
        pass

    @staticmethod
    @abstractmethod
    def count_zeros(columns):
        pass

    @staticmethod
    @abstractmethod
    def count_uniques(columns, estimate=True):
        """
        Result
        {'first_name': {'frequency': [{'value': 'LAUREY', 'count': 3},
           {'value': 'GARRIE', 'count': 3},
           {'value': 'ERIN', 'count': 2},
           {'value': 'DEVINDER', 'count': 1}]}}
        :param columns:
        :param estimate:
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def value_counts(columns):
        pass

    @staticmethod
    @abstractmethod
    def unique(columns):
        pass

    @staticmethod
    @abstractmethod
    def nunique(*args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def select_by_dtypes(data_type):
        pass

    @staticmethod
    @abstractmethod
    def _math(columns, operator, new_column):
        """
        Helper to process arithmetic operation between columns. If a
        :param columns: Columns to be used to make the calculation
        :param operator: A lambda function
        :return:
        """
        pass

    @staticmethod
    def add(columns, col_name="sum"):
        """
        Add two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """

        return BaseColumns._math(columns, lambda x, y: x + y, col_name)

    @staticmethod
    def sub(columns, col_name="sub"):
        """
        Subs two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """
        return BaseColumns._math(columns, lambda x, y: x - y, col_name)

    @staticmethod
    def mul(columns, col_name="mul"):
        """
        Multiply two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """
        return BaseColumns._math(columns, lambda x, y: x * y, col_name)

    @staticmethod
    def div(columns, col_name="div"):
        """
        Divide two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """
        return BaseColumns._math(columns, lambda x, y: x / y, col_name)

    @staticmethod
    @abstractmethod
    def z_score(input_cols, output_cols=None):
        pass

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

    @staticmethod
    @abstractmethod
    def iqr(columns, more=None, relative_error=None):
        pass

    @staticmethod
    @abstractmethod
    def nest(input_cols, shape="string", separator="", output_col=None):
        pass

    @staticmethod
    @abstractmethod
    def unnest(input_cols, separator=None, splits=None, index=None, output_cols=None, drop=False):
        pass

    @staticmethod
    @abstractmethod
    def cell(column):
        pass

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

        for col_name, _dtype in columns.items():
            python_dtype = profiler_dtype_python[_dtype]
            df.meta.set(f"profile.columns.{col_name}.profiler_dtype", _dtype)

            df.meta.preserve(df, Actions.PROFILER_DTYPE.value, col_name)

            #
            # # For categorical columns we need to transform the series to an object
            # # about doing arithmetical operation
            if df.cols.dtypes(col_name) == "category":
                df[col_name] = df[col_name].astype(object)

            if _dtype == "date":
                df[col_name] = df[col_name].astype('M8[us]')
                df.meta.set(f"profile.columns.{col_name}.profiler_dtype_fotmat", _dtype)

            df = df.cols.cast(col_name, python_dtype)
            # print("_dtype",_dtype)
        return df

    @staticmethod
    @abstractmethod
    def frequency_by_group(columns, n=10, percentage=False, total_rows=None):
        pass

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

    @staticmethod
    @abstractmethod
    def qcut(columns, num_buckets, handle_invalid="skip"):
        pass

    @staticmethod
    @abstractmethod
    def clip(columns, lower_bound, upper_bound):
        pass

    @staticmethod
    @abstractmethod
    def values_to_cols(input_cols):
        pass

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
