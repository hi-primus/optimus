import builtins
import itertools
import re
import string
import unicodedata
from functools import reduce

import pandas as pd
from fastnumbers import fast_float
from multipledispatch import dispatch
from pyspark.ml.feature import Imputer, QuantileDiscretizer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType

# Functions
from optimus.functions import abstract_udf as audf
from optimus.functions import filter_row_by_data_type as fbdt
# Helpers
from optimus.helpers.checkit import is_num_or_str, is_list, is_, is_tuple, is_list_of_dataframes, is_list_of_tuples, \
    is_function, is_one_element, is_type, is_int, is_dict, is_str, has_, is_numeric, is_column_a
from optimus.helpers.columns_expression import match_nulls_integers, match_nulls_strings, match_null, na_agg, zeros_agg, \
    na_agg_integer
from optimus.helpers.constants import PYSPARK_NUMERIC_TYPES, PYTHON_TYPES, PYSPARK_NOT_ARRAY_TYPES, \
    PYSPARK_STRING_TYPES, PYSPARK_ARRAY_TYPES
from optimus.helpers.convert import val_to_list, one_list_to_val
from optimus.helpers.decorators import add_attr
from optimus.helpers.functions \
    import validate_columns_names, parse_columns, format_dict, \
    tuple_to_dict, filter_list, get_spark_dtypes_object, collect_as_list, check_column_numbers, get_output_cols
from optimus.helpers.parser import parse_python_dtypes
from optimus.helpers.raiseit import RaiseIt
# Profiler
from optimus.internals import _bucket_col_name
from optimus.profiler.functions import bucketizer
from optimus.profiler.functions import create_buckets

from optimus.functions import append as _append


def cols(self):
    @add_attr(cols)
    @dispatch(str, object)
    def append(col_name=None, value=None):
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

        df = self

        if is_num_or_str(value):
            value = F.lit(value)
        elif is_list(value):
            value = lit_array(value)
        elif is_tuple(value):
            value = lit_array(list(value))

        if is_(value, F.Column):
            df = df.withColumn(col_name, value)

        return df

    @add_attr(cols)
    @dispatch(list)
    def append(cols_values=None):
        """
        Append a column or a Dataframe to a Dataframe
        :param cols_values: New Column Names and data values
        :type cols_values: List of tuples
        :return:
        """
        df_result = None
        # Append a dataframe
        if is_list_of_dataframes(cols_values):
            dfs = cols_values
            dfs.insert(0, self)
            df_result = _append(dfs, like="columns")

        elif is_list_of_tuples(cols_values):
            df_result = self
            for c in cols_values:
                col_name = c[0]
                value = c[1]
                df_result = df_result.cols.append(col_name, value)

        return df_result

    @add_attr(cols)
    def stratify(columns):
        """
        Like Spark groupby but using filter. This is more effective because not use shuffling
        :param columns:
        :return:
        """

        distinct_values = []
        exprs = []
        for column in columns:
            distinct_values.append([x.get(column) for x in self.cols.unique(column).to_json()])

        product = list(itertools.product(*distinct_values))

        for arg in product:
            # Get columns names
            _cols = [F.lit(a) for a in arg]

            # Construct every column conditional
            _filter = [F.col(c) == v for c, v in zip(columns, arg)]

            # Merge column conditional using and operator
            r = F.when(reduce(lambda a, b: a & b, _filter), True)

            # Apply the aggregation function
            agg_func = F.count(r)

            # Assemble the whole expresion
            expr = F.array(*_cols, agg_func)

            exprs.append(expr.alias(reduce(lambda a, b: str(a) + str(b), arg)))

        pdf0 = self.agg(*exprs).toPandas()

        pdf = pdf0.transpose()

        # Unnest array
        pdf = pd.DataFrame(pdf[0].values.tolist(), index=pdf.index)

        num_columns = len(pdf.columns) - 1

        # Convert to int
        pdf = pdf.astype({num_columns: int})

        # Remove count 0
        pdf = pdf[pdf[num_columns] != 0]
        print(pdf)

    @add_attr(cols)
    def select(columns=None, regex=None, data_type=None):
        """
        Select columns using index, column name, regex to data type
        :param columns:
        :param regex:
        :param data_type:
        :return:
        """
        columns = parse_columns(self, columns, is_regex=regex, filter_by_column_dtypes=data_type)
        # Spark can not handle point character in columns names so We enclose the name with `` to ensure
        # columns = escape_columns(columns)

        return self.select(columns)

    @add_attr(cols)
    def apply_expr(input_cols, func=None, args=None, filter_col_by_dtypes=None, output_cols=None, verbose=True):
        """
        Apply a expression to column.
        :param input_cols: Columns in which the function is going to be applied
        :param func: Function to be applied to the data
        :type func: A plain expression or a function
        :param args: Argument passed to the function
        :param filter_col_by_dtypes: Only apply the filter to specific type of value ,integer, float, string or bool
        :param output_cols: Columns in which the transformed data will saved
        :param verbose: Print additional information about
        :return: Dataframe
        """

        return apply(input_cols, func=func, args=args, filter_col_by_dtypes=filter_col_by_dtypes,
                     output_cols=output_cols, verbose=verbose)

    @add_attr(cols)
    def apply(input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
              filter_col_by_dtypes=None, output_cols=None,
              verbose=True):
        """
        Apply a function using pandas udf or udf if apache arrow is not available
        :param input_cols: Columns in which the function is going to be applied
        :param output_cols: Columns in which the transformed data will saved
        :param func: Functions to be applied to a columns. The declaration must have always 2 params.
            def func(value, args):
        :param func_return_type: function return type. This is required by UDF and Pandas UDF.
        :param args: Arguments to be passed to the function
        :param func_type: pandas_udf or udf. If none try to use pandas udf (Pyarrow needed)
        :param when: A expression to better control when the function is going to be apllied
        :param filter_col_by_dtypes: Only apply the filter to specific type of value ,integer, float, string or bool
        :param verbose: Print additional information about
        :return: DataFrame
        """

        input_cols = parse_columns(self, input_cols, filter_by_column_dtypes=filter_col_by_dtypes,
                                   accepts_missing_cols=True)

        check_column_numbers(input_cols, "*")

        output_cols = get_output_cols(input_cols, output_cols)

        df = self

        def expr(_when):
            main_query = audf(input_col, func, func_return_type, args, func_type, verbose=verbose)
            if when is not None:
                # Use the data type to filter the query
                main_query = F.when(_when, main_query).otherwise(F.col(input_col))

            return main_query

        for input_col, output_col in zip(input_cols, output_cols):
            df = df.withColumn(output_col, expr(when))
        return df

    @add_attr(cols)
    def apply_by_dtypes(columns, func, func_return_type, args=None, func_type=None, data_type=None):
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
        columns = parse_columns(self, columns)

        for col_name in columns:
            df = self.cols.apply(col_name, func=func, func_return_type=func_return_type, args=args, func_type=func_type,
                                 when=fbdt(col_name, data_type))
        return df

    # TODO: Check if we must use * to select all the columns
    @add_attr(cols)
    @dispatch(object, object)
    def rename(columns_old_new=None, func=None):
        """"
        Changes the name of a column(s) dataFrame.
        :param columns_old_new: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
        :param func: can be lower, upper or any string transformation function
        """

        df = self

        # Apply a transformation function
        if is_function(func):
            exprs = [F.col(c).alias(func(c)) for c in df.columns]
            df = df.select(exprs)

        elif is_list_of_tuples(columns_old_new):
            # Check that the 1st element in the tuple is a valid set of columns

            validate_columns_names(self, columns_old_new)
            for col_name in columns_old_new:
                old_col_name = col_name[0]
                if is_str(old_col_name):
                    df = df.withColumnRenamed(old_col_name, col_name[1])
                elif is_int(old_col_name):
                    df = df.withColumnRenamed(self.schema.names[old_col_name], col_name[1])

        return df

    @add_attr(cols)
    @dispatch(list)
    def rename(columns_old_new=None):
        return rename(columns_old_new, None)

    @add_attr(cols)
    @dispatch(object)
    def rename(func=None):
        return rename(None, func)

    @add_attr(cols)
    @dispatch(str, str, object)
    def rename(old_column, new_column, func=None):
        return rename([(old_column, new_column)], func)

    @add_attr(cols)
    @dispatch(str, str)
    def rename(old_column, new_column):
        return rename([(old_column, new_column)], None)

    # TODO: Maybe should be possible to cast and array of integer for example to array of double
    @add_attr(cols)
    def cast(input_cols=None, dtype=None, output_cols=None, columns=None):
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
        :return: Spark DataFrame
        """

        _dtype = []
        # Parse params
        if columns is None:
            input_cols = parse_columns(self, input_cols)
            if is_list(input_cols) or is_one_element(input_cols):

                output_cols = get_output_cols(input_cols, output_cols)

                for _ in builtins.range(0, len(input_cols)):
                    _dtype.append(dtype)
        else:

            input_cols = list([c[0] for c in columns])
            if len(columns[0]) == 2:
                output_cols = get_output_cols(input_cols, output_cols)
                _dtype = list([c[1] for c in columns])
            elif len(columns[0]) == 3:
                output_cols = list([c[1] for c in columns])
                _dtype = list([c[2] for c in columns])

            output_cols = get_output_cols(input_cols, output_cols)

        # Helper function to return
        def cast_factory(cls):

            # Parse to Vector
            if is_type(cls, Vectors):
                _func_type = "udf"

                def _cast_to(val, attr):
                    return Vectors.dense(val)

                _func_return_type = VectorUDT()
            # Parse standard data types
            elif get_spark_dtypes_object(cls):

                _func_type = "column_exp"

                def _cast_to(col_name, attr):
                    return F.col(col_name).cast(get_spark_dtypes_object(cls))

                _func_return_type = None

            # Add here any other parse you want
            else:
                RaiseIt.value_error(cls)

            return _func_return_type, _cast_to, _func_type

        df = self

        for input_col, output_col, data_type in zip(input_cols, output_cols, _dtype):
            return_type, func, func_type = cast_factory(data_type)

            df = df.cols.apply(input_col, func, func_return_type=return_type, args=data_type, func_type=func_type,
                               output_cols=output_col, verbose=False)

        return df

    @add_attr(cols)
    @add_attr(cols)
    def astype(*args, **kwargs):
        """
        Like pandas helper
        :param args:
        :param kwargs:
        :return:
        """
        return cast(*args, **kwargs)

    @add_attr(cols)
    def move(column, position, ref_col=None):
        """
        Move a column to specific position
        :param column: Column to be moved
        :param position: Column new position. Accepts 'after', 'before', 'beginning', 'end'
        :param ref_col: Column taken as reference
        :return: Spark DataFrame
        """
        # Check that column is a string or a list
        column = parse_columns(self, column)
        ref_col = parse_columns(self, ref_col)

        # Get dataframe columns
        columns = self.columns

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

        return self[columns]

    @add_attr(cols)
    def keep(columns=None, regex=None):
        """
        Only keep the columns specified
        :param columns: Columns to Keep in the dataFrame
        :param regex: Regular expression to filter
        :return: Spark DataFrame
        """

        if regex:
            r = re.compile(regex)
            columns = list((r.match, self.columns))

        columns = parse_columns(self, columns)
        return self.select(*columns)

    @add_attr(cols)
    # TODO: Create a function to sort by datatype?
    def sort(order="asc"):
        """
        Sort dataframes columns asc or desc
        :param order: 'asc' or 'desc' accepted
        :return: Spark DataFrame
        """

        if order == "asc":
            sorted_col_names = sorted(self.columns)
        elif order == "desc":
            sorted_col_names = sorted(self.columns, reverse=True)
        else:
            RaiseIt.value_error(order, ["asc", "desc"])

        return self.select(sorted_col_names)

    @add_attr(cols)
    def drop(columns=None, regex=None, data_type=None):
        """
        Drop a list of columns
        :param columns: Columns to be dropped
        :param regex: Regex expression to select the columns
        :param data_type:
        :return:
        """
        df = self
        if regex:
            r = re.compile(regex)
            columns = list((r.match, self.columns))

        columns = parse_columns(self, columns, filter_by_column_dtypes=data_type)
        check_column_numbers(columns, "*")

        for col_name in columns:
            df = df.drop(col_name)
        return df

    @add_attr(cols, log_time=True)
    def _exprs(funcs, columns):
        """
        Helper function to apply multiple columns expression to multiple columns
        :param funcs: Aggregation functions from Apache Spark
        :param columns: list or string of columns names or a .
        :return:
        """

        def parse_col_names_funcs_to_keys(data):
            """
            Helper function that return a formatted json with function:value inside columns. Transform from
            {'max_antiguedad_anos': 15,
            'max_m2_superficie_construida': 1800000,
            'min_antiguedad_anos': 2,
            'min_m2_superficie_construida': 20}

            to

            {'m2_superficie_construida': {'min': 20, 'max': 1800000}, 'antiguedad_anos': {'min': 2, 'max': 15}}

            :param data: json data
            :return: json
            """
            functions_array = ["min", "max", "stddev", "kurtosis", "mean", "skewness", "sum", "variance",
                               "approx_count_distinct", "na", "zeros", "percentile"]
            _result = {}
            if is_dict(data):
                for k, v in data.items():
                    for f in functions_array:
                        temp_func_name = f + "_"
                        if k.startswith(temp_func_name):
                            _col_name = k[len(temp_func_name):]
                            # If the value is numeric only get 5 decimals
                            if is_numeric(v):
                                v = round(v, 5)
                            _result.setdefault(_col_name, {})[f] = v
            else:
                if is_numeric(data):
                    data = round(data, 5)
                _result = data

            return _result

        # Ensure that is a list
        funcs = val_to_list(funcs)

        columns = parse_columns(self, columns)

        df = self

        # Create a Column Expression for every column
        expression = []
        for col_name in columns:
            for func in funcs:
                # print(col_name, is_column_a(df, col_name, "date"), func is F.stddev)
                # Std dev is can not process date columns. So we do not calculated

                if not ((func in [F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance, zeros_agg]) and (
                        is_column_a(df, col_name, "date"))):
                    # A different function must be use to calculate null in integers or data column data types
                    if func is na_agg and is_column_a(df, col_name, PYSPARK_NUMERIC_TYPES):
                        func = na_agg_integer

                    expression.append(func(col_name).alias(func.__name__ + "_" + col_name))

            # print(expression)
        result = parse_col_names_funcs_to_keys(format_dict(df.agg(*expression).to_json()))
        return result

    # Quantile statistics
    @add_attr(cols)
    def min(columns):
        """
        Return the min value from a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        return _exprs(F.min, columns)

    @add_attr(cols)
    def max(columns):
        """
        Return the max value from a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        return _exprs(F.max, columns)

    @add_attr(cols)
    def range(columns):
        """
        Return the range form the min to the max value
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        columns = parse_columns(self, columns)

        range_result = {}
        for col_name in columns:
            max_val = self.cols.max(col_name)
            min_val = self.cols.min(col_name)
            range_result[col_name] = {'min': min_val, 'max': max_val}

        return range_result

    @add_attr(cols)
    # TODO: Use pandas or rdd for small datasets?!
    def median(columns, relative_error=1):
        """
        Return the median of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :param relative_error: If set to zero, the exact median is computed, which could be very expensive. 0 to 1 accepted
        :return:
        """
        # columns = parse_columns(self, columns)

        return percentile(columns, [0.5], relative_error)

    @add_attr(cols, log_time=True)
    def percentile(columns, values=None, relative_error=1):
        """
        Return the percentile of a dataframe
        :param columns:  '*', list of columns names or a single column name.
        :param values: list of percentiles to be calculated
        :param relative_error:  If set to zero, the exact percentiles are computed, which could be very expensive. 0 to 1 accepted
        :return: percentiles per columns
        """

        values = val_to_list(values)

        # Make sure values are double
        values = list(map(fast_float, values))
        if values is None:
            values = [0.05, 0.25, 0.5, 0.75, 0.95]

        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        # Get percentiles
        percentile_results = []

        for col_name in columns:
            percentile_per_col = self \
                .rows.drop_na(col_name) \
                .cols.cast(col_name, "double") \
                .approxQuantile(col_name, values, relative_error)

            # Convert numeric keys to str keys
            values_str = list(map(str, values))

            percentile_results.append(dict(zip(values_str, percentile_per_col)))

        percentile_results = dict(zip(columns, percentile_results))

        return format_dict(percentile_results)

    # Descriptive Analytics
    @add_attr(cols)
    # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
    def mad(columns, more=None):
        """
        Return the Median Absolute Deviation
        :param columns: Column to be processed
        :param more: Return some extra computed values (Median).
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        result = {}
        for col_name in columns:

            _mad = {}

            # return mean(absolute(data - mean(data, axis)), axis)
            median_value = self.cols.median(col_name)

            mad_value = self.select(col_name) \
                .withColumn(col_name, F.abs(F.col(col_name) - median_value)) \
                .cols.median(col_name)

            if more:
                _mad = {"mad": mad_value, "median": median_value}
            else:
                _mad = {"mad": mad_value}

            result[col_name] = _mad

        return format_dict(result)

    @add_attr(cols)
    def std(columns):
        """
        Return the standard deviation of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return _exprs(F.stddev, columns)

    @add_attr(cols)
    def kurt(columns):
        """
        Return the kurtosis of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return _exprs(F.kurtosis, columns)

    @add_attr(cols)
    def mean(columns):
        """
        Return the mean of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return _exprs(F.mean, columns)

    @add_attr(cols)
    def skewness(columns):
        """
        Return the skewness of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return _exprs(F.skewness, columns)

    @add_attr(cols)
    def sum(columns):
        """
        Return the sum of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return _exprs(F.sum, columns)

    @add_attr(cols)
    def variance(columns):
        """
        Return the column variance
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return _exprs(F.variance, columns)

    @add_attr(cols)
    def abs(columns):
        """
        Apply abs to the values in a column
        :param columns:
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        df = self
        for col_name in columns:
            df = df.withColumn(col_name, F.abs(F.col(col_name)))
        return df

    @add_attr(cols)
    def mode(columns):
        """
        Return the the column mode
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        columns = parse_columns(self, columns)
        mode_result = []

        for col_name in columns:
            count = self.groupBy(col_name).count()
            mode_df = count.join(
                count.agg(F.max("count").alias("max_")), F.col("count") == F.col("max_")
            )

            # if none of the values are repeated we not have mode
            mode_list = (mode_df
                         .rows.select(mode_df["count"] > 1)
                         .cols.select(col_name)
                         .collect())

            mode_result.append({col_name: filter_list(mode_list)})

        return format_dict(mode_result)

    # String Operations
    @add_attr(cols)
    def lower(input_cols, output_cols=None):
        """
        Lowercase all the string in a column
        :param input_cols: '*', list of columns names or a single column name
        :param output_cols:
        :return:
        """

        def _lower(col, args):
            return F.lower(F.col(col))

        return apply(input_cols, _lower, filter_col_by_dtypes="string", output_cols=output_cols)

    @add_attr(cols)
    def upper(input_cols, output_cols=None):
        """
        Uppercase all the strings column
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _upper(col, args):
            return F.upper(F.col(col))

        return apply(input_cols, _upper, filter_col_by_dtypes="string", output_cols=output_cols)

    @add_attr(cols)
    def trim(input_cols, output_cols=None):
        """
        Trim the string in a column
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _trim(col_name, args):
            return F.trim(F.col(col_name))

        return apply(input_cols, _trim, filter_col_by_dtypes=PYSPARK_NOT_ARRAY_TYPES, output_cols=output_cols)

    @add_attr(cols)
    def reverse(input_cols, output_cols=None):
        """
        Reverse the order of all the string in a column
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _reverse(col, args):
            return F.reverse(F.col(col))

        df = apply_expr(input_cols, _reverse, filter_col_by_dtypes="string", output_cols=output_cols)

        return df

    @add_attr(cols)
    def remove(columns, search=None, search_by="chars", output_cols=None):
        """
        Remove chars or words
        :param columns: '*', list of columns names or a single column name.
        :param search: values to look at to be replaced
        :param search_by: Match substring or words
        :param output_cols:
        :return:
        """
        return self.cols.replace(columns, search, "", search_by, output_cols)

    @add_attr(cols)
    def remove_accents(input_cols, output_cols=None):
        """
        Remove accents in specific columns
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _remove_accents(value, attr):
            cell_str = str(value)

            # first, normalize strings:
            nfkd_str = unicodedata.normalize('NFKD', cell_str)

            # Keep chars that has no other char combined (i.e. accents chars)
            with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
            return with_out_accents

        df = apply(input_cols, _remove_accents, "string", output_cols=output_cols)
        return df

    @add_attr(cols)
    def remove_special_chars(input_cols, output_cols=None):
        """
        Reference https://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
        This method remove special characters (i.e. !”#$%&/()=?) in columns of dataFrames.
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        input_cols = parse_columns(self, input_cols, filter_by_column_dtypes=PYSPARK_STRING_TYPES)
        check_column_numbers(input_cols, "*")

        df = self.cols.replace(input_cols, [s for s in string.punctuation], "", "chars", output_cols=output_cols)
        return df

    @add_attr(cols)
    def remove_white_spaces(input_cols, output_cols=None):
        """
        Remove all the white spaces from a string
        :param input_cols:
        :param output_cols:
        :return:
        """

        def _remove_white_spaces(col_name, args):
            return F.regexp_replace(F.col(col_name), " ", "")

        df = apply(input_cols, _remove_white_spaces, output_cols=output_cols,
                   filter_col_by_dtypes=PYSPARK_NOT_ARRAY_TYPES)
        return df

    @add_attr(cols)
    def date_transform(input_cols, current_format=None, output_format=None, output_cols=None):
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

        def _date_transform(col_name, attr):
            _current_format = attr[0]
            _output_format = attr[1]

            return F.date_format(F.unix_timestamp(col_name, _current_format).cast("timestamp"), _output_format).alias(
                col_name)

        # Asserting if column if in dataFrame:
        df = apply(input_cols, _date_transform, args=[current_format, output_format], output_cols=output_cols)

        return df

    @add_attr(cols)
    def years_between(input_cols, date_format=None, output_cols=None):
        """
        This method compute the age based on a born date.
        :param input_cols: Name of the column born dates column.
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

        output_cols = get_output_cols(input_cols, output_cols)

        df = apply(input_cols, func=_years_between, args=[date_format],
                   filter_col_by_dtypes=PYSPARK_NOT_ARRAY_TYPES, output_cols=output_cols)

        df = df.cols.cast(output_cols, "float")

        return df

    @add_attr(cols)
    def replace(input_cols, search=None, replace_by=None, search_by="chars", output_cols=None):
        """
        Replace a value, list of values by a specified string
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param search: Values to look at to be replaced
        :param replace_by: New value to replace the old one
        :param search_by: Match substring or words
        :return:
        """

        # TODO check if .contains can be used instead of regexp
        def func_chars(_df, _input_col, _output_col, _search, _replace_by):
            # Reference https://www.oreilly.com/library/view/python-cookbook/0596001673/ch03s15.html

            # Create as dict
            if is_list(search):
                _search_and_replace_by = {s: _replace_by for s in search}
            elif is_one_element(search):
                _search_and_replace_by = {search: _replace_by}

            _search_and_replace_by = {str(k): str(v) for k, v in _search_and_replace_by.items()}

            def multiple_replace(_value, __search_and_replace_by):
                # Create a regular expression from all of the dictionary keys
                if _value is not None:

                    _regex = re.compile("|".join(map(re.escape, __search_and_replace_by.keys())))
                    result = _regex.sub(lambda match: __search_and_replace_by[match.group(0)], str(_value))
                else:
                    result = None
                return result

            return _df.cols.apply(_input_col, multiple_replace, "string", _search_and_replace_by,
                                  output_cols=_output_col)

        def func_words(_df, _col_name, _search, _replace_by):
            _search = val_to_list(search)
            # Convert the value to column data type
            data_type = self.cols.dtypes(_col_name)
            _search = [PYTHON_TYPES[data_type](s) for s in _search]

            return _df.replace(_search, _replace_by, _col_name)

        if search_by is "words":
            func = func_words
        elif search_by is "chars":
            func = func_chars
        else:
            RaiseIt.value_error(search_by, ["words", "chars"])

        input_cols = parse_columns(self, input_cols,
                                   filter_by_column_dtypes=PYSPARK_STRING_TYPES + PYSPARK_NUMERIC_TYPES)
        check_column_numbers(input_cols, "*")
        output_cols = get_output_cols(input_cols, output_cols)

        df = self
        for input_col, output_col in zip(input_cols, output_cols):
            if is_column_a(df, input_col, "int"):
                df = df.cols.cast(input_col, "str", output_col)

            df = func(df, input_col, output_col, search, replace_by)

        return df

    @add_attr(cols)
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
        # TODO check if .contains can be used instead of regexp

        def func_regex(_input_cols, attr):
            _search = attr[0]
            _replace = attr[1]
            return F.regexp_replace(_input_cols, _search, _replace)

        return apply(input_cols, func=func_regex, args=[regex, value], output_cols=output_cols)

    @add_attr(cols)
    def impute(input_cols, data_type="continuous", strategy="mean", output_cols=None):
        """
        Imputes missing data from specified columns using the mean or median.
        :param input_cols: list of columns to be analyze.
        :param output_cols:
        :param data_type: continuous or categorical
        :param strategy: String that specifies the way of computing missing data. Can be "mean", "median" for continuous
        or "mode" for categorical columns
        :return: Dataframe object (DF with columns that has the imputed values).
        """
        df = self

        if data_type is "continuous":
            input_cols = parse_columns(self, input_cols,
                                       filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
            check_column_numbers(input_cols, "*")
            output_cols = get_output_cols(input_cols, output_cols)

            # Imputer require not only numeric but float or double
            # print("{} values imputed for column(s) '{}'".format(df.cols.count_na(input_col), input_col))
            df = df.cols.cast(input_cols, "float", output_cols)
            imputer = Imputer(inputCols=output_cols, outputCols=output_cols)

            model = imputer.setStrategy(strategy).fit(df)
            df = model.transform(df)

        elif data_type is "categorical":

            input_cols = parse_columns(self, input_cols, filter_by_column_dtypes=PYSPARK_STRING_TYPES)
            check_column_numbers(input_cols, "*")
            output_cols = get_output_cols(input_cols, output_cols)

            value = df.cols.mode(input_cols)
            df = df.cols.fill_na(output_cols, value, output_cols)
        else:
            RaiseIt.value_error(data_type, ["continuous", "categorical"])

        return df

    @add_attr(cols)
    def fill_na(input_cols, value=None, output_cols=None):
        """
        Replace null data with a specified value
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :param value: value to replace the nan/None values
        :return:
        """
        input_cols = parse_columns(self, input_cols)
        check_column_numbers(input_cols, "*")
        output_cols = get_output_cols(input_cols, output_cols)

        df = self
        for input_col, output_col in zip(input_cols, output_cols):
            if is_column_a(self, input_col, PYSPARK_NUMERIC_TYPES):
                new_value = fast_float(value)
                func = F.when(match_nulls_strings(input_col), new_value).otherwise(F.col(input_col))
            elif is_column_a(self, input_col, PYSPARK_STRING_TYPES):
                # print(col_name)
                new_value = str(value)
                func = F.when(match_nulls_strings(input_col), new_value).otherwise(F.col(input_col))
            elif is_column_a(self, input_col, PYSPARK_ARRAY_TYPES):
                if is_one_element(value):
                    new_value = F.array(F.lit(value))
                else:
                    new_value = F.array(*[F.lit(v) for v in value])
                func = F.when(match_null(input_col), new_value).otherwise(F.col(input_col))
            else:

                if df.cols.dtypes(input_col) == parse_python_dtypes(type(value).__name__):
                    new_value = value
                    func = F.when(match_null(input_col), new_value).otherwise(F.col(input_col))
                else:
                    RaiseIt.type_error(value, [df.cols.dtypes(input_col)])

            df = df.cols.apply(input_col, func=func, output_cols=output_col)
        return df

    @add_attr(cols)
    def is_na(input_cols, output_cols=None):
        """
        Replace null values with True and non null with False
        :param input_cols: '*', list of columns names or a single column name.
        :param output_cols:
        :return:
        """

        def _replace_na(_col_name, _value):
            return F.when(F.col(_col_name).isNull(), True).otherwise(False)

        return self.cols.apply(input_cols, _replace_na, output_cols=output_cols)

    @add_attr(cols)
    def count():
        """
        Return the columns number
        :return:
        """
        return len(self.columns)

    @add_attr(cols)
    def count_na(columns):
        """
        Return the NAN and Null count in a Column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        columns = parse_columns(self, columns)
        check_column_numbers(columns, "*")

        df = self
        expr = []

        for col_name in columns:

            # If type column is Struct parse to String. isnan/isNull can not handle Structure/Boolean
            if is_column_a(df, col_name, ["struct", "boolean"]):
                df = df.cols.cast(col_name, "string")

            # Select the nan/null rows depending of the columns data type
            # If numeric
            if is_column_a(df, col_name, PYSPARK_NUMERIC_TYPES):
                expr.append(F.count(F.when(match_nulls_integers(col_name), col_name)).alias(col_name))
            # If string. Include 'nan' string
            elif is_column_a(df, col_name, PYSPARK_STRING_TYPES):
                expr.append(F.count(
                    F.when(match_nulls_strings(col_name), col_name)).alias(
                    col_name))
                print("Including 'nan' as Null in processing '{}'".format(col_name))
            else:
                expr.append(F.count(F.when(match_null(col_name), col_name)).alias(col_name))

        result = format_dict(df.select(*expr).to_json())
        return result

    @add_attr(cols)
    def count_zeros(columns):
        """
        Return the NAN and Null count in a Column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        df = self
        return format_dict(df.select(
            [F.count(F.when(F.col(col_name) == 0, col_name)).alias(col_name) for col_name in columns]).to_json())

    @add_attr(cols)
    def count_uniques(columns, estimate=True):
        """
        Return how many unique items exist in a columns
        :param columns: '*', list of columns names or a single column name.
        :param estimate: If true use hyperloglog to estimate distinct count. If False use full distinct
        :type estimate: bool
        :return:
        """
        columns = parse_columns(self, columns)

        if estimate is True:
            result = _exprs(F.approx_count_distinct, columns)
        else:
            df = self
            result = {col_name: df.select(col_name).distinct().count() for col_name in columns}
        return format_dict(result)

    @add_attr(cols)
    def value_counts(columns):
        """
        Return the counts of uniques values
        :param columns:
        :return:
        """
        columns = parse_columns(self, columns)
        # check_column_numbers(columns, 1)

        return self.groupBy(columns).count().orderBy('count')

    @add_attr(cols)
    def unique(columns):
        """
        Return unique values from a columns
        :param columns:
        :return:
        """
        columns = parse_columns(self, columns)
        check_column_numbers(columns, "*")

        result = {}
        for col_name in columns:
            unique_results = self.select(col_name).distinct().to_json()
            uniques = []
            for unique_dict in unique_results:
                for k, v in unique_dict.items():
                    uniques.append(v)
            result[col_name] = uniques

        return result

    @add_attr(cols)
    def nunique(*args, **kwargs):
        """
        Just a pandas compatible shortcut for count uniques
        :param args:
        :param kwargs:
        :return:
        """
        return count_uniques(*args, **kwargs)

    @add_attr(cols)
    def select_by_dtypes(data_type):
        """
        This function returns one or multiple dataFrame columns which match with the data type provided.
        :param data_type: Datatype column to look at
        :return:
        """

        columns = parse_columns(self, '*', is_regex=None, filter_by_column_dtypes=data_type)

        return self.select(columns)

    # Operations between columns
    @add_attr(cols)
    def _math(columns, operator, new_column):
        """
        Helper to process arithmetic operation between columns. If a
        :param columns: Columns to be used to make the calculation
        :param operator: A lambda function
        :return:
        """

        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        df = self
        for col_name in columns:
            df = df.cols.cast(col_name, "float")

        if len(columns) < 2:
            raise Exception("Error: 2 or more columns needed")

        cols = list(map(lambda x: F.col(x), columns))
        expr = reduce(operator, cols)

        return df.withColumn(new_column, expr)

    @add_attr(cols)
    def add(columns, col_name="sum"):
        """
        Add two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """

        return _math(columns, lambda x, y: x + y, col_name)

    @add_attr(cols)
    def sub(columns, col_name="sub"):
        """
        Subs two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """
        return _math(columns, lambda x, y: x - y, col_name)

    @add_attr(cols)
    def mul(columns, col_name="mul"):
        """
        Multiply two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """
        return _math(columns, lambda x, y: x * y, col_name)

    @add_attr(cols)
    def div(columns, col_name="div"):
        """
        Divide two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """
        return _math(columns, lambda x, y: x / y, col_name)

    # Stats
    @add_attr(cols)
    def z_score(input_cols, output_cols=None):
        """
        Return the column data type
        :param input_cols: '*', list of columns names or a single column name
        :param output_cols:
        :return:
        """

        def _z_score(col_name, attr):
            mean_value = self.cols.mean(col_name)
            stdev_value = self.cols.std(col_name)
            return F.abs((F.col(col_name) - mean_value) / stdev_value)

        return apply(input_cols, func=_z_score, filter_col_by_dtypes=PYSPARK_NUMERIC_TYPES, output_cols=output_cols)

    @add_attr(cols)
    def iqr(columns, more=None):
        """
        Return the column data type
        :param columns:
        :param more: Return info about q1 and q3
        :return:
        """
        iqr_result = {}
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        for col_name in columns:
            quartile = self.cols.percentile(col_name, [0.25, 0.5, 0.75], relative_error=0)
            q1 = quartile["0.25"]
            q2 = quartile["0.5"]
            q3 = quartile["0.75"]

            iqr_value = q3 - q1
            if more:
                result = {"iqr": iqr_value, "q1": q1, "q2": q2, "q3": q3}
            else:
                result = iqr_value
            iqr_result[col_name] = result

        return format_dict(iqr_result)

    @add_attr(cols)
    # TODO: Maybe we should create nest_to_vector and nest_array, nest_to_string
    def nest(input_cols, shape="string", separator="", output_cols=None):
        """
        Concat multiple columns to one with the format specified
        :param input_cols: columns to be nested
        :param separator: char to be used as separator at the concat time
        :param shape: final data type, 'array', 'string' or 'vector'
        :param output_cols:
        :return: Spark DataFrame
        """

        df = self

        if has_(input_cols, F.Column):
            # Transform non Column data to lit
            input_cols = [F.lit(col) if not is_(col, F.Column) else col for col in input_cols]
        else:
            input_cols = parse_columns(self, input_cols)

        if shape is "vector":
            input_cols = parse_columns(self, input_cols, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)

            check_column_numbers(input_cols, "*")
            vector_assembler = VectorAssembler(
                inputCols=input_cols,
                outputCol=output_cols)
            df = vector_assembler.transform(df)

        elif shape is "array":
            # Arrays needs all the elements with the same data type. We try to cast to type
            df = df.cols.cast("*", "str")
            df = df.cols.apply(input_cols, F.array(*input_cols), output_cols=output_cols)

        elif shape is "string":
            df = df.cols.apply(input_cols, F.concat_ws(separator, *input_cols), output_cols=output_cols)
        else:
            RaiseIt.value_error(shape, ["vector", "array", "string"])

        return df

    @add_attr(cols)
    def unnest(input_cols, separator=None, splits=None, index=None, output_cols=None):
        """
        Split an array or string in different columns
        :param input_cols: Columns to be un-nested
        :param output_cols:
        :param separator: char or regex
        :param splits: Number of rows to un-nested. Because we can not know beforehand the number of splits
        :param index: Return a specific index from the nest
        :return: Spark DataFrame
        """

        # If a number of split was not defined try to infer the length with the first element
        infer_splits = None
        if splits is None:
            infer_splits = True

        input_cols = parse_columns(self, input_cols)
        output_cols = get_output_cols(input_cols, output_cols)

        df = self

        for input_col, output_col in zip(input_cols, output_cols):
            # Array
            if is_column_a(df, input_col, ArrayType):

                expr = F.col(input_col)
                # Try to infer the array length using the first row
                if infer_splits is True:
                    splits = format_dict(self.agg(F.max(F.size(input_col))).to_json())

                for i in builtins.range(splits):
                    df = df.withColumn(output_col + "_" + str(i), expr.getItem(i))

            # String
            elif is_column_a(df, input_col, StringType):
                if separator is None:
                    RaiseIt.value_error(separator, "regular expression")

                # Try to infer the array length using the first row
                if infer_splits is True:
                    splits = format_dict(self.agg(F.max(F.size(F.split(F.col(input_col), separator)))).to_json())

                if is_int(index):
                    r = builtins.range(index, index + 1)
                else:
                    r = builtins.range(0, splits)

                expr = F.split(F.col(input_col), separator)
                for i in r:
                    df = df.withColumn(output_col + "_" + str(i), expr.getItem(i))

            # Vector
            elif is_column_a(df, input_col, VectorUDT):

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

        return df

    @add_attr(cols)
    def cell(column):
        """
        Get the value for the first cell from a column in a data frame
        :param column: Column to be processed
        :return:
        """
        return self.cols.select(column).first()[0]

    @add_attr(cols)
    def scatter(columns, buckets=10):
        """
        Return scatter plot data in json format
        :param columns:
        :param buckets: number of buckets
        :return:
        """

        if len(columns) != 2:
            RaiseIt.length_error(columns, "2")

        columns = parse_columns(self, columns)
        df = self
        for col_name in columns:
            values = _exprs([F.min, F.max], columns)

            # Create splits
            splits = create_buckets(values[col_name]["min"], values[col_name]["max"], buckets)

            # Create buckets in the dataFrame
            df = bucketizer(df, col_name, splits=splits, output_cols=_bucket_col_name(col_name))

        columns_bucket = [_bucket_col_name(col_name) for col_name in columns]

        size_name = "count"
        result = df.groupby(columns_bucket).agg(F.count('*').alias(size_name),
                                                F.round((F.max(columns[0]) + F.min(columns[0])) / 2).alias(columns[0]),
                                                F.round((F.max(columns[1]) + F.min(columns[1])) / 2).alias(columns[1]),
                                                ).rows.sort(columns).toPandas()
        x = result[columns[0]].tolist()
        y = result[columns[1]].tolist()
        s = result[size_name].tolist()

        return {"x": {"name": columns[0], "data": x}, "y": {"name": columns[1], "data": y}, "s": s}

    @add_attr(cols, log_time=True)
    @dispatch((str, list), (float, int), (float, int), int)
    def hist(columns, min_value, max_value, buckets=10):
        """
         Get the histogram column in json format
        :param columns: Columns to be processed
        :param min_value: Min value used to calculate the buckets
        :param max_value: Max value used to calculate the buckets
        :param buckets: Number of buckets
        :return:
        """

        columns = parse_columns(self, columns)
        for col_name in columns:
            # Create splits
            splits = create_buckets(min_value, max_value, buckets)

            # Create buckets in the dataFrame
            df = bucketizer(self, input_cols=col_name, splits=splits, output_cols=_bucket_col_name(col_name))

            col_bucket = _bucket_col_name(col_name)

            counts = (df
                      .h_repartition(col_name=col_bucket)
                      .groupBy(col_bucket)
                      .agg(F.count(col_bucket).alias("count"))
                      .cols.rename(col_bucket, "value")
                      .sort(F.asc("value")).to_json())

            # Fill the gaps in dict values. For example if we have  1,5,7,8,9 it get 1,2,3,4,5,6,7,8,9
            new_array = []
            for i in builtins.range(buckets):
                flag = False
                for c in counts:
                    value = c["value"]
                    count = c["count"]
                    if value == i:
                        new_array.append({"value": value, "count": count})
                        flag = True
                if flag is False:
                    new_array.append({"value": i, "count": 0})

            counts = new_array

            hist_data = []
            for i in list(itertools.zip_longest(counts, splits)):
                if i[0] is None:
                    count = 0
                elif "count" in i[0]:
                    count = i[0]["count"]

                lower = i[1]["lower"]
                upper = i[1]["upper"]

                hist_data.append({"count": count, "lower": lower, "upper": upper})

        return hist_data

    @add_attr(cols, log_time=True)
    @dispatch((str, list), int)
    def hist(columns, buckets=10):
        values = _exprs([F.min, F.max], columns)
        return self.cols.hist(columns, fast_float(values[columns]["min"]), fast_float(values[columns]["max"]), buckets)

    @add_attr(cols)
    def frequency(columns, buckets=10):
        """
        Output values frequency in json format
        :param columns: Columns to be processed
        :param buckets: Number of buckets
        :return:
        """
        columns = parse_columns(self, columns)
        df = self

        result = {}
        for col_name in columns:
            result[col_name] = df.groupBy(col_name).count().rows.sort([("count", "desc"), (col_name, "desc")]).limit(
                buckets).cols.rename(col_name, "value").to_json()

        return result

    @add_attr(cols)
    def boxplot(columns):
        """
        Output values frequency in json format
        :param columns: Columns to be processed
        :return:
        """
        columns = parse_columns(self, columns)
        df = self

        for col_name in columns:
            iqr = df.cols.iqr(col_name, more=True)
            lb = iqr["q1"] - (iqr["iqr"] * 1.5)
            ub = iqr["q3"] + (iqr["iqr"] * 1.5)

            mean = df.cols.mean(columns)

            query = ((F.col(col_name) < lb) | (F.col(col_name) > ub))
            fliers = collect_as_list(df.rows.select(query).cols.select(col_name).limit(1000))
            stats = [{'mean': mean, 'med': iqr["q2"], 'q1': iqr["q1"], 'q3': iqr["q3"], 'whislo': lb, 'whishi': ub,
                      'fliers': fliers, 'label': one_list_to_val(col_name)}]

            return stats

    @add_attr(cols)
    def schema_dtype(columns="*"):
        """
        Return the column(s) data type as Type
        :param columns: Columns to be processed
        :return:
        """
        columns = parse_columns(self, columns)
        return format_dict([self.schema[col_name].dataType for col_name in columns])

    @add_attr(cols)
    def dtypes(columns="*"):
        """
        Return the column(s) data type as string
        :param columns: Columns to be processed
        :return:
        """

        columns = parse_columns(self, columns)
        data_types = tuple_to_dict(self.dtypes)

        return format_dict({col_name: data_types[col_name] for col_name in columns})

    @add_attr(cols)
    def names(col_names="*", filter_by_column_dtypes=None, invert=False):
        """
        Get columns names
        :param col_names: Columns names to be processed '*' for all or a list of column names
        :param filter_by_column_dtypes: Data type used to select the columns
        :param invert: Invert the columns selection
        :return:
        """
        columns = parse_columns(self, col_names, filter_by_column_dtypes=filter_by_column_dtypes, invert=invert)
        return columns

    @add_attr(cols)
    def qcut(columns, num_buckets, handle_invalid="skip"):
        """
        Bin columns into n buckets. Quantile Discretizer
        :param columns: Input columns to processed
        :param num_buckets: Number of buckets in which the column will be divided
        :param handle_invalid:
        :return:
        """
        df = self
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        for col_name in columns:
            output_col = col_name + "_qcut"
            discretizer = QuantileDiscretizer(numBuckets=num_buckets, inputCol=col_name, outputCol=output_col,
                                              handleInvalid=handle_invalid)
            df = discretizer.fit(df).transform(df)
        return df

    @add_attr(cols)
    def clip(columns, lower_bound, upper_bound):
        """
        Trim values at input thresholds
        :param columns: Columns to be trimmed
        :param lower_bound: Lower value bound
        :param upper_bound: Upper value bound
        :return:
        """

        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        def _clip(_col_name, args):
            _lower = args[0]
            _upper = args[1]
            return (F.when(F.col(_col_name) <= _lower, _lower)
                    .when(F.col(_col_name) >= _upper, _upper)).otherwise(F.col(col_name))

        df = self
        for col_name in columns:
            df = df.cols.apply_expr(col_name, _clip, [lower_bound, upper_bound])
        return df

    return cols


DataFrame.cols = property(cols)
