import builtins
import os
import re
import sys
import unicodedata

import pyspark
from multipledispatch import dispatch
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, QuantileDiscretizer, StringIndexer, IndexToString
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import when

import optimus.helpers.functions_spark
from optimus import ROOT_DIR
from optimus.engines.base.distributed.columns import DistributedBaseColumns
from optimus.engines.base.meta import Meta
# Helpers
from optimus.engines.base.pandas.columns import PandasBaseColumns
from optimus.helpers.check import has_, is_column_a, is_spark_dataframe
from optimus.helpers.columns import get_output_cols, parse_columns, check_column_numbers, name_col, prepare_columns
from optimus.helpers.constants import RELATIVE_ERROR, Actions
from optimus.helpers.converter import format_dict
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.functions \
    import create_buckets
from optimus.helpers.functions_spark import append as append_df
from optimus.helpers.logger import logger
from optimus.helpers.parser import compress_list
from optimus.helpers.raiseit import RaiseIt
from optimus.helpers.types import *

# Add the directory containing your module to the Python path (wants absolute paths)
from optimus.profiler.constants import MAX_BUCKETS

sys.path.append(os.path.abspath(ROOT_DIR))

# To use this functions as a Spark udf function we need to load it using addPyFile because the file can not be loaded
# as python module because it generate a pickle error.
# from infer import Infer

from optimus.infer import is_, is_list_value, is_tuple, is_list_of_str, \
    is_list_of_tuples, is_one_element, is_num_or_str, is_numeric, is_str, is_int
# from optimus.infer_spark import SPARK_DTYPES_TO_INFERRED, parse_spark_class_dtypes, is_list_of_spark_dataframes
# NUMERIC_TYPES, NOT_ARRAY_TYPES, STRING_TYPES, ARRAY_TYPES
from optimus.engines.spark.audf import filter_row_by_data_type as fbdt
from optimus.engines.base.ml.constants import STRING_TO_INDEX, INDEX_TO_STRING

# Functions

ENGINE = "spark"


class Cols(PandasBaseColumns, DistributedBaseColumns):

    def _names(self):
        return list(self.root.data.columns)

    @dispatch(str, object)
    def append(self, col_name=None, value=None):
        """
        Append a column to a Dataframe
        :param col_name: Name of the new column
        :param value: List of data values
        :return:
        """

        def lit_array(_value):
            temp = []
            for v in _value:
                temp.append(F.lit(v))
            return F.array(temp)

        dfd = self.root.data

        if is_num_or_str(value):
            value = F.lit(value)
        elif is_list_value(value):
            value = lit_array(value)
        elif is_tuple(value):
            value = lit_array(list(value))

        if is_(value, F.Column):
            dfd = dfd.withColumn(col_name, value)

        return self.root.new(dfd)

    @dispatch((list, pyspark.sql.dataframe.DataFrame))
    def append(self, cols_values=None):
        """
        Append a column or a Dataframe to a Dataframe
        :param cols_values: New Column Names and data values
        :type cols_values: List of tuples
        :return:
        """
        df = self.root
        df_result = None
        if is_list_of_tuples(cols_values):

            for c in cols_values:
                col_name = c[0]
                value = c[1]
                df_result = optimus.helpers.functions_spark.append(col_name, value)

        elif is_list_of_spark_dataframes(cols_values) or is_spark_dataframe(cols_values):
            cols_values = val_to_list(cols_values)
            cols_values.insert(0, df)
            df_result = append_df(cols_values, like="columns")

        else:
            RaiseIt.type_error(cols_values, ["list of tuples", "dataframes"])

        return df_result

    def copy(self, input_cols, output_cols=None, columns=None):
        """
        Copy one or multiple columns
        :param input_cols: Source column to be copied
        :param output_cols: Destination column
        :param columns: tuple of column [('column1','column_copy')('column1','column1_copy')()]
        :return:
        """
        df = self.root

        if is_list_of_str(columns):
            output_cols = [col_name + "_copy" for col_name in columns]
            output_cols = get_output_cols(columns, output_cols)
            columns = zip(columns, output_cols)
        else:
            input_cols = parse_columns(dfd, input_cols)
            output_cols = get_output_cols(input_cols, output_cols)

            columns = list(zip(input_cols, output_cols))

        for input_col, output_col in columns:
            current_meta = dfd.meta.get()
            dfd = df.data.withColumn(output_col, F.col(input_col))
            meta.set(value=current_meta)
            meta = Meta.action(meta, Actions.COPY.value, (input_col, output_col))

        return self.root.new(dfd, meta=meta)

    @staticmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        """
        Convert a string to timestamp
        :param input_cols:
        :param output_cols:
        :param date_format:
        :return:
        """
        df = self.root

        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        for input_col, output_col in zip(input_cols, output_cols):
            df = df.withColumn(output_col, F.to_timestamp(input_col, date_format))
        return df

    # def apply(self, input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
    #           filter_col_by_dtypes=None, output_cols=None, skip_output_cols_processing=False,
    #           meta_action="apply", default=None, mode=None):
    #     """
    #     Apply a function using pandas udf or udf if apache arrow is not available
    #     :param input_cols: Columns in which the function is going to be applied
    #     :param output_cols: Columns in which the transformed data will saved
    #     :param func: Functions to be applied to a columns. The declaration must have always 2 params.
    #         def func(value, args):
    #     :param func_return_type: function return type. This is required by UDF and Pandas UDF.
    #     :param args: Arguments to be passed to the function
    #     :param func_type: pandas_udf or udf. If none try to use pandas udf (Pyarrow needed)
    #     :param when: A expression to better control when the function is going to be applied
    #     :param filter_col_by_dtypes: Only apply the filter to specific type of value ,integer, float, string or bool
    #     :param skip_output_cols_processing: In some special cases we do not want apply() to construct the output columns.
    #     True or False
    #     :param meta_action: Metadata transformation to a dataframe
    #     """
    #     func_return_type = {str: "string", int: "int", float: "float", bool: "boolean"}.get(func_return_type)
    #     df = self.root
    #     columns = prepare_columns(df, input_cols, output_cols, filter_by_column_types=filter_col_by_dtypes,
    #                               accepts_missing_cols=True, default=default)
    #
    #     def expr(_when):
    #         main_query = audf(input_col, func, func_return_type, args, func_type)
    #         if when is not None:
    #             # Use the data type to filter the query
    #             main_query = F.when(_when, main_query).otherwise(F.col(input_col))
    #
    #         return main_query
    #
    #     dfd = df.data
    #     meta = df.meta
    #     for input_col, output_col in columns:
    #         # print("expr(when)",type(expr(when)),expr(when))
    #         dfd = dfd.withColumn(output_col, expr(when))
    #         meta = Meta.action(meta, meta_action, output_col)
    #         # self.root.meta.preserve(self, meta_action, output_col)
    #     # dfd = self.root.new(dfd)
    #
    #     return self.root.new(dfd)

    @staticmethod
    def apply_by_data_type(columns, func, func_return_type, args=None, func_type=None, data_type=None):
        """
        Apply a function using pandas udf or udf if apache arrow is not available
        :param columns: Columns in which the function is going to be applied
        :param func: Functions to be applied to a columns
        :param func_return_type
        :param args:
        :param func_type: pandas_udf or udf. If none try to use pandas udf (Pyarrow needed)
        :param data_type:
        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)
        for col_name in columns:
            df = df.cols.apply(col_name, func=func, func_return_type=func_return_type, args=args,
                               func_type=func_type,
                               when=fbdt(col_name, data_type))
        return df

    # TODO: Maybe we could merge this with apply()
    def set(self, output_cols=None, value=None, where=None, default=None, eval_value=False):
        """
        Set a column value using a number, string or a expression.
        :param where:
        :param value:
        :param output_cols:
        :param default:
        :return:
        """
        df = self.root

        columns = parse_columns(df, output_cols, accepts_missing_cols=True)
        check_column_numbers(columns, 1)

        if is_list_value(value):
            expr = F.array([F.lit(x) for x in value])
        elif is_numeric(value):
            expr = F.lit(value)
        elif is_str(value):
            expr = F.expr(value)
        else:
            RaiseIt.value_error(value, ["numeric", "list", "hive expression"])

        df = df.withColumn(output_cols, expr)
        df.meta = Meta.action(df.meta, None, Actions.SET.value, columns)
        return df

    def create_exprs(self, columns, funcs, *args, df=None):
        """
        Helper function to apply multiple columns expression to multiple columns
        :param columns:
        :param funcs:
        :param args:
        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)
        funcs = val_to_list(funcs)
        exprs = []

        for col_name in columns:
            for func in funcs:
                exprs.append((func, (col_name, *args, df)))
        # print("exprs",exprs)
        # df = self

        # Std, kurtosis, mean, skewness and other agg functions can not process date columns.
        filters = {"date": [F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance, F.approx_count_distinct,
                            self.F.zeros_agg],
                   "array": [F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance, F.approx_count_distinct,
                             self.F.zeros_agg],
                   "timestamp": [F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance,
                                 F.approx_count_distinct,
                                 self.F.zeros_agg, self.F.percentile],
                   "null": [F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance, F.approx_count_distinct,
                            self.F.zeros_agg],
                   "boolean": [F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance, F.approx_count_distinct,
                               self.F.zeros_agg],
                   "binary": [F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance, F.approx_count_distinct,
                              self.F.zeros_agg]
                   }

        def _filter(_col_name, _func):
            for data_type, func_filter in filters.items():
                for f in func_filter:
                    if (_func == f) and (is_column_a(df, _col_name, data_type)):
                        return True
            return False

        beauty_col_names = {"hist_agg": "hist", "percentile": "percentile", "zeros_agg": "zeros",
                            "count_na_agg": "count_na", "range_agg": "range", "count_uniques_agg": "count_uniques"}

        def _beautify_col_names(_func):
            if _func.__name__ in beauty_col_names:
                func_name = beauty_col_names[_func.__name__]
            else:
                func_name = _func.__name__
            return func_name

        def _agg_exprs(_funcs):
            _exprs = []
            for f in _funcs:
                _func = f[0]
                _args = f[1]
                _col_name = _args[0]

                if not _filter(_col_name, _func):
                    agg = _func(*_args)
                    if agg is not None:
                        func_name = _beautify_col_names(_func)
                        _exprs.append(agg.alias(func_name + "_" + _col_name))

            return _exprs

        r = _agg_exprs(exprs)

        return r

    # # Descriptive Analytics
    # @staticmethod
    # # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
    # def mad(columns, relative_error=RELATIVE_ERROR, more=None):
    #     """
    #     Return the Median Absolute Deviation
    #     :param columns: Column to be processed
    #     :param more: Return some extra computed values (Median).
    #     :param relative_error: Relative error calculating the media
    #     :return:
    #     """
    #     columns = parse_columns(
    #         self.root, columns, filter_by_column_types=self.root.constants.NUMERIC_TYPES)
    #     check_column_numbers(columns, "*")
    #
    #     result = {}
    #     for col_name in columns:
    #
    #         _mad = {}
    #         median_value = self.root.cols.median(col_name, relative_error)
    #         mad_value = self.root.data.withColumn(col_name, F.abs(F.col(col_name) - median_value)) \
    #             .cols.median(col_name, relative_error)
    #
    #         if more:
    #             _mad = {"mad": mad_value, "median": median_value}
    #         else:
    #             _mad = {"mad": mad_value}
    #
    #         result[col_name] = _mad
    #
    #     return format_dict(result)

    @staticmethod
    def reverse(columns):
        pass

    # String Operations

    def remove(self, columns, search=None, search_by="chars", output_cols=None):
        """
        Remove chars or words
        :param columns: '*', list of columns names or a single column name.
        :param search: values to look at to be replaced
        :param search_by: Match substring or words
        :param output_cols:
        :return:
        """
        return self.replace(columns, search, "", search_by, output_cols)

    def normalize_chars(self, input_cols="*", output_cols=None):
        """
        Remove accents in specific columns
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _normalize_chars(value):
            value = str(value)

            # first, normalize strings:
            nfkd_str = unicodedata.normalize('NFKD', value)

            # Keep chars that has no other char combined (i.e. accents chars)
            with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
            return with_out_accents

        df = self.apply(input_cols, _normalize_chars, str, output_cols=output_cols,
                        meta_action=Actions.REMOVE_ACCENTS.value)
        return df

    def remove_special_chars(self, input_cols="*", output_cols=None):
        """
        Reference https://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
        This method remove special characters (i.e. !”#$%&/()=?) in columns of dataFrames.
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        input_cols = parse_columns(
            self.root, input_cols, filter_by_column_types=self.root.constants.STRING_TYPES)
        check_column_numbers(input_cols, "*")

        df = self.replace(input_cols, [f"\\{s}" for s in string.punctuation], "", "chars", output_cols=output_cols)
        return df

    def remove_white_spaces(self, input_cols="*", output_cols=None):
        """
        Remove all the white spaces from a string
        :param input_cols:
        :param output_cols:
        :return:
        """

        def _remove_white_spaces(col_name, args):
            return F.regexp_replace(F.col(col_name), " ", "")

        return self.apply(input_cols, _remove_white_spaces, output_cols=output_cols,
                          filter_col_by_dtypes=self.root.constants.NOT_ARRAY_TYPES,
                          meta_action=Actions.REMOVE_WHITE_SPACES.value)

    @staticmethod
    def format_date(input_cols, current_format=None, output_format=None, output_cols=None):
        """
        Transform a column date to a specified format
        :param input_cols: Columns to be transformed.
        :param output_cols: Output columns
        :param current_format: Current_format is the current string dat format of columns specified. Of course,
                                all columns specified must have the same format. Otherwise the function is going
                                to return tons of null values because the transformations in the columns with
                                different formats will fail. For example "yyyy/MM/dd"
        :param output_format: Output date string format to be expected. For example "dd-MM-YYYY"
        """

        def _date_format(col_name, attr):
            _current_format = attr[0]
            _output_format = attr[1]

            return F.date_format(F.unix_timestamp(col_name, _current_format).cast("timestamp"),
                                 _output_format).alias(
                col_name)

        # Asserting if column if in dataFrame:
        df = Cols.apply(input_cols, _date_format, args=[current_format, output_format], output_cols=output_cols)

        return df

    @staticmethod
    def years_between(input_cols, date_format=None, output_cols=None):
        """
        Years between a date and now
        :param input_cols: Input columns
        :param output_cols:
        :param date_format: String format date of the column provided. For example "yyyyMMdd"
        """

        # Output format date
        format_dt = "yyyy-MM-dd"

        def _years_between(col_name, attr):
            _date_format = attr[0]

            return F.format_number(
                F.abs(
                    F.months_between(
                        F.date_format(
                            F.unix_timestamp(
                                col_name,
                                _date_format).cast("timestamp"),
                            format_dt),
                        F.current_date()) / 12), 4) \
                .alias(
                col_name)

        # output_cols = get_output_cols(input_cols, output_cols)

        df = Cols.apply(input_cols, func=_years_between, args=[date_format],
                        filter_col_by_dtypes=self.root.constants.NOT_ARRAY_TYPES, output_cols=output_cols)

        df = df.cols.cast(output_cols, "float")

        return df

    def match(self, input_cols, regex=None, value=None, output_cols=None):
        """
        Use a Regex to replace values
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param regex: values to look at to be replaced
        :param value: new value to replace the old one
        :return:
        """

        # if regex or normal replace we use regexp or replace functions
        def _match(_input_cols, attr):
            _search = attr[0]
            return F.when(F.col(_input_cols).rlike(_search), True).otherwise(False)

        return self.apply(input_cols, func=_match, args=(regex,), func_type="column_expr", func_return_type=str,
                          output_cols=output_cols,
                          meta_action=Actions.REPLACE_REGEX.value)

    def replace(self, input_cols, search=None, replace_by=None, search_by="chars", output_cols=None):
        """
        Replace a value, list of values by a specified string
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param search: Values to look at to be replaced
        :param replace_by: New value to replace the old one
        :param search_by: Can be "full","words","chars" or "numeric".
        :return:
        """

        # TODO check if .contains can be used instead of regexp
        def func_chars_words(_df, _input_col, _output_col, _search, _replace_by):
            # Reference https://www.oreilly.com/library/view/python-cookbook/0596001673/ch03s15.html

            # Create as dict
            _search_and_replace_by = None
            if is_list_value(search):
                _search_and_replace_by = {s: _replace_by for s in search}
            elif is_one_element(search):
                _search_and_replace_by = {search: _replace_by}

            _search_and_replace_by = {str(k): str(v) for k, v in _search_and_replace_by.items()}
            _regex = re.compile("|".join(map(re.escape, _search_and_replace_by.keys())))

            def multiple_replace(_value, __search_and_replace_by):
                # Create a regular expression from all of the dictionary keys
                if _value is not None:
                    __regex = None
                    if search_by == "chars":
                        __regex = re.compile("|".join(map(re.escape, __search_and_replace_by.keys())))
                    elif search_by == "words":
                        __regex = re.compile(
                            r'\b%s\b' % r'\b|\b'.join(map(re.escape, __search_and_replace_by.keys())))
                    result = __regex.sub(lambda match: __search_and_replace_by[match.group(0)], str(_value))

                else:
                    result = None

                return result

            return self.apply(_input_col, multiple_replace, "str", (_search_and_replace_by,), output_cols=_output_col)

        def func_full(_df, _input_col, _output_col, _search, _replace_by):
            _search = val_to_list(search)

            if _input_col != output_col:
                _df = _df.cols.copy(_input_col, _output_col)

            return _df.replace(_search, _replace_by, _output_col)

        def func_numeric(_df, _input_col, _output_col, _search, _replace_by):
            _df = _df.withColumn(_output_col,
                                 F.when(df[_input_col] == _search, _replace_by).otherwise(df[_output_col]))
            return _df

        func = None
        if search_by == "full":
            func = func_full
        elif search_by == "chars" or search_by == "words":
            func = func_chars_words
        elif search_by == "numeric":
            func = func_numeric
        else:
            RaiseIt.value_error(search_by, ["chars", "words", "full", "numeric"])

        filter_dtype = None

        df = self.root

        if search_by in ["chars", "words", "full"]:
            filter_dtype = [df.constants.STRING_TYPES]
        elif search_by == "numeric":
            filter_dtype = [df.constants.NUMERIC_TYPES]

        columns = prepare_columns(df.data, input_cols, output_cols)
        # columns = prepare_columns(df.data, input_cols, output_cols, filter_by_column_types=filter_dtype)
        dfd = df.data
        for input_col, output_col in columns:
            dfd = func(dfd, input_col, output_col, search, replace_by)

            meta = Meta.action(df.meta, None, Actions.REPLACE.value, output_col)
        return self.root.new(dfd, meta=meta)

    @staticmethod
    def replace_regex(input_cols, regex=None, value=None, output_cols=None):
        """
        Use a Regex to replace values
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param regex: values to look at to be replaced
        :param value: new value to replace the old one
        :return:
        """

        # if regex or normal replace we use regexp or replace functions
        def func_regex(_input_cols, attr):
            _search = attr[0]
            _replace = attr[1]
            return F.regexp_replace(_input_cols, _search, _replace)

        return Cols.apply(input_cols, func=func_regex, args=[regex, value], output_cols=output_cols,
                          filter_col_by_dtypes=self.root.constants.STRING_TYPES + self.root.constants.NUMERIC_TYPES,
                          meta_action=Actions.REPLACE_REGEX.value)

    def impute(self, cols="*", data_type="continuous", strategy="mean", fill_value=0, output_cols=None):
        """
        Imputes missing data from specified columns using the mean or median.
        :param fill_value:
        :param cols: list of columns to be analyze.
        :param output_cols:
        :param data_type: "continuous" or "categorical"
        :param strategy: String that specifies the way of computing missing data. Can be "mean", "median" for continuous
        or "mode" for categorical columns
        :return: Dataframe object (DF with columns that has the imputed values).
        """
        df = self.root

        strategy_options = ["mean", "median", "mode"]
        meta = Meta.action(df.meta, Actions.IMPUTE.value, cols)
        if data_type == "continuous":
            input_cols = parse_columns(df, cols)
            output_cols = get_output_cols(input_cols, output_cols)

            # Imputer require not only numeric but float or double
            if strategy == "most_frequent":
                strategy = "mode"

            if strategy not in strategy_options:
                RaiseIt.value_error(strategy, strategy_options)

            dfd = df.cols.cast(input_cols, "float", output_cols).data.to_spark()
            imputer = Imputer(inputCols=output_cols, outputCols=output_cols)
            model = imputer.setStrategy(strategy).setMissingValue(fill_value).fit(dfd)

            df = self.root.new(model.transform(dfd).to_koalas(), meta=meta)

        elif data_type == "categorical":

            input_cols = parse_columns(df, cols,
                                       filter_by_column_types=df.constants.STRING_TYPES)
            output_cols = get_output_cols(input_cols, output_cols)
            value = df.cols.mode(input_cols)
            df = df.cols.fill_na(output_cols, value=value, output_cols=output_cols)

            df = self.root.new(df.data.to_koalas(), meta=meta)

        else:
            RaiseIt.value_error(data_type, ["continuous", "categorical", "auto"])
        return df

    def is_na(self, input_cols, output_cols=None):
        """
        Replace null values with True and non null with False
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _replace_na(_col_name, _value):
            return F.when(F.col(_col_name).isNull(), True).otherwise(False)

        return self.apply(input_cols, _replace_na, output_cols=output_cols, meta_action=Actions.IS_NA.value)

    @staticmethod
    def unique(columns):
        """
        Return uniques values from a columns
        :param columns:
        :return:
        """
        columns = parse_columns(self.root, columns)

        # .value(columns, "1")

        result = {}
        for col_name in columns:
            result.update(compress_list(self.select(col_name).distinct().to_dict()))
        return result

    # Stats
    # @staticmethod
    # def z_score(input_cols="*", output_cols=None):
    #     """
    #     Return the column z score
    #     :param input_cols: '*', list of columns names or a single column name
    #     :param output_cols:
    #     :return:
    #     """
    #
    #     df = self.root
    #
    #     def _z_score(col_name, attr):
    #         mean_value = df.cols.mean(col_name)
    #         stdev_value = df.cols.std(col_name)
    #         return F.abs((F.col(col_name) - mean_value) / stdev_value)
    #
    #     input_cols = parse_columns(df, input_cols)
    #
    #     # Hint the user if the column has not the correct data type
    #     for input_col in input_cols:
    #         if not is_column_a(df, input_col, df.constants.NUMERIC_TYPES):
    #             print(
    #                 "'{}' column is not numeric, z-score can not be calculated. Cast column to numeric using df.cols.cast()".format(
    #                     input_col))
    #
    #     return Cols.apply(input_cols, func=_z_score, filter_col_by_dtypes=df.constants.NUMERIC_TYPES,
    #                       output_cols=output_cols,
    #                       meta_action=Actions.Z_SCORE.value)

    @staticmethod
    def standard_scaler(input_cols, output_cols=None):
        return 1

    @staticmethod
    def min_max_scaler(input_cols, output_cols=None):
        """
        Return the column min max scaler result
        :param input_cols: '*', list of columns names or a single column name
        :param output_cols:
        :return:
        """

        # Spark suuport this by default. But this implemntation support multiple columns
        def _min_max(col_name, attr):
            range_value = self.root.cols.range(col_name)
            min_value = range_value[col_name]["range"]["min"]
            max_value = range_value[col_name]["range"]["max"]
            return F.abs((F.col(col_name) - min_value) / max_value - min_value)

        return Cols.apply(input_cols, func=_min_max, filter_col_by_dtypes=self.root.constants.NUMERIC_TYPES,
                          output_cols=output_cols,
                          meta_action=Actions.MIN_MAX_SCALER.value)

    @staticmethod
    def max_abs_scaler(input_cols, output_cols=None):
        """
        Return the max abs scaler result
        :param input_cols: '*', list of columns names or a single column name
        :param output_cols:
        :return:
        """

        # Spark suuport this by default. But this implemntation support multiple columns
        def _result(col_name, attr):
            def max_abs(col_name):
                return F.max(F.abs(F.col(col_name)))

            max_abs_result = format_dict(Cols.agg_exprs(input_cols, max_abs))

            return (F.col(col_name)) / max_abs_result

        return Cols.apply(input_cols, func=_result, filter_col_by_dtypes=self.root.constants.NUMERIC_TYPES,
                          output_cols=output_cols,
                          meta_action=Actions.MAX_ABS_SCALER.value)

    def create_key(self, col="id") -> 'DataFrameType':
        dfd = self.data.withColumn(col, F.monotonically_increasing_id())
        return self.root.new(dfd)

    @staticmethod
    # TODO: Maybe we should create nest_to_vector and nest_array, nest_to_string
    def nest(input_cols, separator="", output_col=None, drop=True, shape="string"):
        """
        Concat multiple columns to one with the format specified
        :param input_cols: columns to be nested
        :param separator: char to be used as separator at the concat time
        :param shape: final data type, 'array', 'string' or 'vector'
        :param output_col:
        :return: Spark DataFrame
        """
        df = self.root
        output_col = parse_columns(df, output_col, accepts_missing_cols=True)
        check_column_numbers(output_col, 1)

        output_ordered_columns = df.cols.names()

        if output_col not in output_ordered_columns:
            output_ordered_columns.append(output_col)

        if has_(input_cols, F.Column):
            # Transform non Column data to lit
            input_cols = [F.lit(col) if not is_(col, F.col) else col for col in input_cols]
        else:
            input_cols = parse_columns(df, input_cols)

        if shape == "vector":
            input_cols = parse_columns(df, input_cols,
                                       filter_by_column_types=df.constants.NUMERIC_TYPES)
            output_col = one_list_to_val(output_col)
            vector_assembler = VectorAssembler(
                inputCols=input_cols,
                outputCol=output_col)

            df = vector_assembler.transform(df)

            df.meta = Meta.action(df.meta, None, Actions.NEST.value, output_col)

        elif shape == "array":
            # Arrays needs all the elements with the same data type. We try to cast to type
            df = df.cols.cast("*", "str")
            df = df.cols.apply(input_cols, F.array(*input_cols), output_cols=output_col,
                               skip_output_cols_processing=True, meta_action=Actions.NEST.value)

        elif shape == "string":
            df = df.cols.apply(input_cols, F.concat_ws(separator, *input_cols), output_cols=output_col,
                               skip_output_cols_processing=True, meta_action=Actions.NEST.value)
        else:
            RaiseIt.value_error(shape, ["vector", "array", "string"])

        df.meta = Meta.action(df.meta, None, Actions.NEST.value, output_col)

        if output_col not in output_ordered_columns:
            col_index = output_ordered_columns.index(input_cols[-1]) + 1
            output_ordered_columns[col_index:col_index] = [output_col]

        if drop is True:
            for input_col in input_cols:
                if input_col in output_ordered_columns and input_col != output_col:
                    output_ordered_columns.remove(input_col)

        return df.cols.select(output_ordered_columns)

    @staticmethod
    def unnest(input_cols, separator=None, splits=None, index=None, output_cols=None, drop=False) -> DataFrame:
        """
        Split an array or string in different columns
        :param input_cols: Columns to be un-nested
        :param output_cols: Resulted on or multiple columns  after the unnest operation [(output_col_1_1,output_col_1_2), (output_col_2_1, output_col_2]
        :param separator: char or regex
        :param splits: Number of rows to un-nested. Because we can not know beforehand the number of splits
        :param index: Return a specific index per columns. [{1,2},()]
        :param drop:
        """

        # If a number of split was not defined try to infer the length with the first element
        infer_splits = None
        if splits is None:
            infer_splits = True

        # Special case. A dot must be escaped
        if separator is not None:
            separator = re.escape(separator)

        df = self.root

        input_cols = parse_columns(df, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)
        final_columns = None

        def _final_columns(_index, _splits, _output_col):

            if _index is None:
                actual_index = builtins.range(0, _splits)
            else:
                _index = val_to_list(_index)

                if is_list_of_tuples(_index):
                    _index = [(i - 1, j - 1) for i, j in _index]
                elif is_list_value(_index):
                    _index = [i - 1 for i in _index]

                actual_index = _index

            # Create final output columns
            if is_tuple(_output_col):
                columns = zip(actual_index, _output_col)
            else:
                columns = [(i, _output_col + "_" + str(i)) for i in actual_index]
            return columns

        for idx, (input_col, output_col) in enumerate(zip(input_cols, output_cols)):

            # If numeric convert and parse as string.
            if is_column_a(df, input_col, df.constants.NUMERIC_TYPES):
                df = df.cols.cast(input_col, "str")

            # Parse depending of data types
            if is_column_a(df, input_col, "struct"):
                # Unnest a data Struct
                df = df.select(output_col + ".*")

            # Array
            elif is_column_a(df, input_col, "array"):
                # Try to infer the array length using the first row
                if infer_splits is True:
                    splits = format_dict(df.agg(F.max(F.size(input_col))).to_dict())

                expr = F.col(input_col)
                final_columns = _final_columns(index, splits, output_col)
                for i, col_name in final_columns:
                    df = df.withColumn(col_name, expr.getItem(i))

            # String
            elif is_column_a(df, input_col, "str"):
                if separator is None:
                    RaiseIt.value_error(separator, "regular expression")

                # Try to infer the array length using the first row
                if infer_splits is True:
                    splits = format_dict(df.agg(F.max(F.size(F.split(F.col(input_col), separator)))).to_dict())

                expr = F.split(F.col(input_col), separator)
                final_columns = _final_columns(index, splits, output_col)
                for i, col_name in final_columns:
                    df = df.withColumn(col_name, expr.getItem(i))

            # Vector
            # TODO: Maybe we could implement Pandas UDF for better control columns output
            elif is_column_a(df, input_col, "vector"):

                def _unnest(row):
                    _dict = row.asDict()

                    # Get the column we want to unnest
                    _list = _dict[input_col]

                    # Ensure that float are python floats and not np floats
                    if index is None:
                        _list = [float(x) for x in _list]
                    else:
                        _list = [float(_list[1])]

                    return row + tuple(_list)

                df = df.rdd.map(_unnest).toDF(df.columns)

            else:
                RaiseIt.type_error(input_col, ["string", "struct", "array", "vector"])
            df.meta = Meta.action(df.meta, None, Actions.UNNEST.value, [v for k, v in final_columns])
        return df

    @staticmethod
    def heatmap(columns, buckets=10):
        """
        Returns heat map data in a dictionary
        :param columns:
        :param buckets: number of buckets
        :return:
        """

        if len(columns) != 2:
            RaiseIt.length_error(columns, "2")

        df = self.root
        columns = parse_columns(df, columns)

        values = df.cols.range(columns)

        for col_name in columns:
            # Create splits
            splits = create_buckets(values[col_name]["range"]["min"], values[col_name]["range"]["max"], buckets)

            # Create buckets in the dataFrame
            df = df.cols.bucketizer(col_name, splits=splits, output_cols=name_col(col_name, "bucketizer"))

        columns_bucket = [name_col(col_name, "bucketizer") for col_name in columns]

        size_name = "count"
        result = df.groupby(columns_bucket).agg(F.count('*').alias(size_name),
                                                F.round((F.max(columns[0]) + F.min(columns[0])) / 2).alias(
                                                    columns[0]),
                                                F.round((F.max(columns[1]) + F.min(columns[1])) / 2).alias(
                                                    columns[1]),
                                                ).rows.sort(columns).toPandas()
        x = result[columns[0]].tolist()
        y = result[columns[1]].tolist()
        s = result[size_name].tolist()

        return {"x": {"name": columns[0], "data": x}, "y": {"name": columns[1], "data": y}, "s": s}

    def hist(self, cols="*", buckets=MAX_BUCKETS, range=None, compute=True) -> dict:
        df = self.root
        # return df.cols.agg_exprs(columns, self.F.min, compute=compute, tidy=True)

        return df.cols.agg_exprs(cols, self.F.hist, buckets=buckets, range=range, compute=compute)
        # return df.cols.agg_exprs(cols, self.F.mad, relative_error, more, estimate, compute=compute, tidy=tidy)
        # TODO: for some reason casting to int in the exprs do not work. Casting Here. A Spark bug?
        # Example
        # Column < b'array(map(count, CAST(sum(CASE WHEN ((rank >= 7) AND (rank < 7.75)) THEN 1 ELSE 0 END) AS INT),
        # lower, 7, upper, 7.75) AS `hist_agg_rank_0`, map(count, CAST(sum(CASE WHEN ((rank >= 7.75) AND (rank < 8.5))
        # THEN 1 ELSE 0 END) AS INT), lower, 7.75, upper, 8.5) AS `hist_agg_rank_1`, map(count,
        # CAST(sum(CASE WHEN ((rank >= 8.5) AND (rank < 9.25)) THEN 1 ELSE 0 END) AS INT), lower, 8.5, upper, 9.25)
        # AS `hist_agg_rank_2`, map(count, CAST(sum(CASE WHEN ((rank >= 9.25) AND (rank < 10))
        # THEN 1 ELSE 0 END) AS INT), lower, 9.25, upper, 10) AS `hist_agg_rank_3`) AS `histrank`' >

        return {"hist": result}

        # TODO: In tests this code run faster than using Cols.agg_exprs when run over all the columns.
        #  Not when running over columns individually
        # columns = parse_columns(self, columns)
        # df = self
        # for col_name in columns:
        #     print(Cols.agg_exprs(hist_agg, col_name, self, buckets))
        #     # print(df.agg(hist_agg(col_name, self, buckets)))
        # return result

    def correlation(self, input_cols, method="pearson", output="json"):
        """
        Calculate the correlation between columns. It will try to cast a column to float where necessary and impute
        missing values
        :param input_cols: Columns to be processed
        :param method: Method used to calculate the correlation
        :param output: array or json
        :return:
        """

        df = self.root

        # Values in columns can not be null. Warn user
        input_cols = parse_columns(
            df, input_cols, filter_by_column_types=df.constants.NUMERIC_TYPES)

        # Input is not a vector transform to a vector
        output_col = name_col(input_cols, "correlation")
        check_column_numbers(input_cols, ">1")

        for col_name in input_cols:
            df = df.cols.cast(col_name, "float")
            logger.print("Casting {col_name} to float...".format(col_name=col_name))

        df = df.cols.nest(input_cols, "vector", output_col=output_col)

        # Correlation can not handle null values. Check if exist ans warn the user.
        cols_na = {x: y for x, y in df.cols.count_na(input_cols).items() if y != 0}

        if cols_na:
            message = "Correlation can not handle nulls. " + " and ".join(
                {str(x) + " has " + str(y) + " null(s)" for x, y in cols_na.items()})
            RaiseIt.message(ValueError, message)

        corr = Correlation.corr(df, output_col, method).head()[0].toArray()
        result = None
        if output == "array":
            result = corr

        elif output == "json":
            # Parse result to json
            col_pair = []
            for col_name in input_cols:
                for col_name_2 in input_cols:
                    col_pair.append({"between": col_name, "an": col_name_2})

            # flat array
            values = corr.flatten('F').tolist()

            result = []
            for n, v in zip(col_pair, values):
                # Remove correlation between the same column
                if n["between"] is not n["an"]:
                    n["value"] = v
                    result.append(n)

            result = sorted(result, key=lambda k: k['value'], reverse=True)

        return {"cols": input_cols, "data": result}

    # def schema_data_type(self, columns="*"):
    #     """
    #     Return the column(s) data type as Type
    #     :param columns: Columns to be processed
    #     :return:
    #     """
    #     df = self.root
    #     columns = parse_columns(df, columns)
    #     return format_dict([df.data.schema[col_name].dataType for col_name in columns])

    def qcut(self, columns, quantiles, handle_invalid="skip"):
        """
        Bin columns into n buckets. Quantile Discretizer
        :param columns: Input columns to processed
        :param num_buckets: Number of buckets in which the column will be divided
        :param handle_invalid:
        :return:
        """
        df = self.root
        columns = parse_columns(
            df, columns, filter_by_column_types=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        for col_name in columns:
            discretizer = QuantileDiscretizer(numBuckets=quantiles, inputCol=col_name,
                                              outputCol=name_col(col_name, "qcut"),
                                              handleInvalid=handle_invalid)
            df = discretizer.fit(df).transform(df)
        return df

    @staticmethod
    def clip(columns, lower_bound, upper_bound):
        """
        Trim values at input thresholds
        :param columns: Columns to be trimmed
        :param lower_bound: Lower value bound
        :param upper_bound: Upper value bound
        :return:
        """

        df = self.root

        columns = parse_columns(
            df, columns, filter_by_column_types=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        def _clip(_col_name, args):
            _lower = args[0]
            _upper = args[1]
            return (F.when(F.col(_col_name) <= _lower, _lower)
                    .when(F.col(_col_name) >= _upper, _upper)).otherwise(F.col(col_name))

        for col_name in columns:
            df = df.cols.apply_expr(col_name, _clip, [lower_bound, upper_bound])
        return df

    def string_to_index(self, cols=None, output_cols=None, columns=None, **kwargs):
        """
        Encodes a string column of labels to a column of label indices
        :param cols:
        :param output_cols:
        :param columns:
        :return:
        """

        df = self.root

        if columns is None:
            input_cols = parse_columns(df, cols)
            if output_cols is None:
                output_cols = [name_col(input_col, STRING_TO_INDEX) for input_col in input_cols]
            output_cols = get_output_cols(input_cols, output_cols)
        else:
            input_cols, output_cols = zip(*columns)

        dfd = df.data.to_spark()
        indexers = [StringIndexer(inputCol=input_col, outputCol=output_col, handleInvalid="skip", **kwargs).fit(dfd) for
                    input_col, output_col
                    in zip(list(set(input_cols)), list(set(output_cols)))]

        pipeline = Pipeline(stages=indexers)
        dfd = pipeline.fit(dfd).transform(dfd)

        meta = Meta.action(df.meta, Actions.STRING_TO_INDEX.value, input_cols)
        df = self.root.new(dfd.to_koalas(), meta=meta)
        return df

    def index_to_string(self, cols=None, output_cols=None, columns=None, **kwargs):
        """
        Encodes a string column of labels to a column of label indices
        :param cols:
        :param output_cols:
        :param columns:
        :return:
        """
        df = self.root

        if columns is None:
            input_cols = parse_columns(df, cols)
            if output_cols is None:
                output_cols = [name_col(input_col, INDEX_TO_STRING) for input_col in input_cols]
            output_cols = get_output_cols(input_cols, output_cols)
        else:
            input_cols, output_cols = zip(*columns)

        dfd = df.data.to_spark()
        indexers = [IndexToString(inputCol=input_col, outputCol=output_col, **kwargs) for input_col, output_col
                    in zip(list(set(input_cols)), list(set(output_cols)))]
        pipeline = Pipeline(stages=indexers)
        dfd = pipeline.fit(dfd).transform(dfd)

        meta = Meta.action(df.meta, Actions.STRING_TO_INDEX.value, input_cols)
        df = self.root.new(dfd.to_koalas(), meta=meta)
        return df

    @staticmethod
    def bucketizer(input_cols, splits, output_cols=None):
        """
        Bucketize multiples columns at the same time.
        :param input_cols:
        :param splits: Dict of splits or ints. You can use create_buckets() to make it
        :param output_cols:
        :return:
        """
        df = self.root

        if is_int(splits):
            min_max = df.cols.range(input_cols)[input_cols]["range"]
            splits = create_buckets(min_max["min"], min_max["max"], splits)

        def _bucketizer(col_name, args):
            """
            Create a column expression that create buckets in a range of values
            :param col_name: Column to be processed
            :return:
            """

            buckets = args
            expr = []

            for i, b in enumerate(buckets):
                if i == 0:
                    expr = when((F.col(col_name) >= b["lower"]) & (F.col(col_name) <= b["upper"]), b["bucket"])
                else:
                    expr = expr.when((F.col(col_name) >= b["lower"]) & (F.col(col_name) <= b["upper"]),
                                     b["bucket"])

            return expr

        df = df.cols.apply(input_cols, func=_bucketizer, args=splits, output_cols=output_cols)
        return df
