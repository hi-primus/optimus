import builtins
import itertools
import re
import string
import timeit
import unicodedata
from fastnumbers import fast_float
from functools import reduce

from multipledispatch import dispatch
from pyspark.ml.feature import Imputer, QuantileDiscretizer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import Column

from optimus.functions import abstract_udf as audf, concat
from optimus.functions import filter_row_by_data_type as fbdt
from optimus.helpers.checkit \
    import is_num_or_str, is_list, is_, is_tuple, is_list_of_dataframes, is_list_of_tuples, \
    is_function, is_one_element, is_type, is_int, is_dict, is_str, is_
# Helpers
from optimus.helpers.constants import *
from optimus.helpers.decorators import add_attr
from optimus.helpers.functions \
    import validate_columns_names, parse_columns, format_dict, \
    tuple_to_dict, val_to_list, filter_list, get_spark_dtypes_object
from optimus.helpers.raiseit import RaiseIt
from optimus.profiler.functions import bucketizer
from optimus.profiler.functions import create_buckets


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

        def lit_array(value):
            temp = []
            for v in value:
                temp.append(F.lit(v))
            return F.array(temp)

        df = self

        if is_num_or_str(value):
            value = F.lit(value)
        elif is_list(value):
            value = lit_array(value)
        elif is_tuple(value):
            value = lit_array(list(value))

        if is_(value, Column):
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

        # Append a dataframe
        if is_list_of_dataframes(cols_values):
            dfs = cols_values
            dfs.insert(0, self)
            df_result = concat(dfs, like="columns")

        elif is_list_of_tuples(cols_values):
            df_result = self
            for c in cols_values:
                col_name = c[0]
                value = c[1]
                df_result = df_result.cols.append(col_name, value)

        return df_result

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
        return self.select(columns)

    @add_attr(cols)
    def apply_expr(columns, func=None, args=None, filter_col_by_dtypes=None, verbose=True):
        """
        Apply a expression to column.
        :param columns: Columns in which the function is going to be applied
        :param func: function to be applied
        :type func: A plain expression or a function
        :param args: Argument passed to the function
        :param filter_col_by_dtypes: Only apply the filter to specific type of value ,integer, float, string or bool
        :param verbose: Print additional information about
        :return: Dataframe
        """

        # It handle if func param is a plain expression or a function returning and expression
        def func_col_exp(col_name, attr):
            return func

        if is_(func, Column):
            _func = func_col_exp
        else:
            _func = func

        columns = parse_columns(self, columns, filter_by_column_dtypes=filter_col_by_dtypes, accepts_missing_cols=True)

        df = self
        for col_name in columns:
            df = df.withColumn(col_name, audf(col_name, _func, attrs=args, func_type="column_exp", verbose=verbose))
        return df

    @add_attr(cols)
    def apply(columns, func, func_return_type, args=None, func_type=None, when=None, filter_col_by_dtypes=None,
              verbose=True):
        """
        Apply a function using pandas udf or udf if apache arrow is not available
        :param columns: Columns in which the function is going to be applied
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

        columns = parse_columns(self, columns, filter_by_column_dtypes=filter_col_by_dtypes, accepts_missing_cols=True)

        df = self

        def expr(_when):
            main_query = audf(c, func, func_return_type, args, func_type, verbose=verbose)
            if when is not None:
                # Use the data type to filter the query
                main_query = F.when(_when, main_query).otherwise(F.col(c))

            return main_query

        for c in columns:
            df = df.withColumn(c, expr(when))
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

        for c in columns:
            df = self.cols.apply(c, func, func_return_type, args=args, func_type=func_type,
                                 when=fbdt(c, data_type))
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
            for c in columns_old_new:
                old_col_name = c[0]
                if is_str(old_col_name):
                    df = df.withColumnRenamed(old_col_name, c[1])
                elif is_int(old_col_name):
                    df = df.withColumnRenamed(self.schema.names[old_col_name], c[1])

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

    def _cast(cols, args):
        """
        Helper function to support the multiple params implementation
        :param cols:
        :param args:
        :return:
        """

        # assert validate_columns_names(self, cols_and_types, 0)
        # cols, attrs = parse_columns(self, cols_and_dtypes, get_args=True)

        # if parse_spark_dtypes(attr[0])
        def cast_factory(cls):

            # Parse to Vector
            if is_type(cls, Vectors):
                func_type = "udf"

                def cast_to_vectors(val, attr):
                    return Vectors.dense(val)

                func_return_type = VectorUDT()
            # Parse standard data types
            elif get_spark_dtypes_object(cls):

                func_type = "column_exp"

                def cast_to_vectors(col_name, attr):
                    return F.col(col_name).cast(get_spark_dtypes_object(cls))

                func_return_type = None

            # Add here any other parse you want
            else:
                RaiseIt.value_error(cls)

            return func_return_type, cast_to_vectors, func_type

        df = self
        for col, args in zip(cols, args):
            return_type, func, func_type = cast_factory(args[0])
            df = df.withColumn(col, audf(col, func,
                                         func_return_type=return_type,
                                         attrs=args[0],
                                         func_type=func_type, verbose=False)
                               )
        return df

    @add_attr(cols)
    @dispatch(list)
    def cast(col_and_dtype):
        """
        Cast multiple columns to a specific datatype
        List of tuples of column names and types to be casted. This variable should have the
                following structure:

                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]

                The first parameter in each tuple is the column name, the second is the final datatype of column after
                the transformation is made.

        :param col_and_dtype: Columns to be casted and new data types
        :return:
        """
        cols, attrs = parse_columns(self, col_and_dtype, get_args=True)
        return _cast(cols, attrs)

    @add_attr(cols)
    @dispatch((list, str), object)
    def cast(columns, dtype):
        """
        Cast a column or a list of columns to a specific datatype
        :param columns: Columns names to be casted
        :param dtype: final data type
        :return: Spark DataFrame
        """

        cols = parse_columns(self, columns)
        attrs = []
        for _ in builtins.range(0, len(cols)):
            attrs.append((dtype,))

        return _cast(cols, attrs)

    @add_attr(cols)
    @add_attr(cols)
    def astype(*args, **kwargs):
        return cast(*args, **kwargs)

    @add_attr(cols)
    def move(column, position, ref_col):
        """
        Move a column to specific position
        :param column: Column to be moved
        :param ref_col: Column taken as reference
        :param position: Column new position. Accepts 'after' or 'before'
        :return: Spark DataFrame
        """
        # Check that column is a string or a list
        column = parse_columns(self, column)
        ref_col = parse_columns(self, ref_col)

        # Asserting if position is 'after' or 'before'
        assert (position == 'after') or (
                position == 'before'), "Error: Position parameter only can be 'after' or 'before' actually" % position

        # Get dataframe columns
        columns = self.columns

        # Get source and reference column index position

        new_index = columns.index(ref_col[0])
        old_index = columns.index(column[0])

        # if position is 'after':
        if position == 'after':
            # Check if the movement is from right to left:
            if new_index >= old_index:
                columns.insert(new_index, columns.pop(old_index))  # insert and delete a element
            else:  # the movement is form left to right:
                columns.insert(new_index + 1, columns.pop(old_index))
        else:  # If position if before:
            if new_index[0] >= old_index:  # Check if the movement if from right to left:
                columns.insert(new_index - 1, columns.pop(old_index))
            elif new_index[0] < old_index:  # Check if the movement if from left to right:
                columns.insert(new_index, columns.pop(old_index))

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

        for column in columns:
            df = df.drop(column)
        return df

    @add_attr(cols)
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
            result = {}
            if is_dict(data):
                for k, v in data.items():
                    for f in functions_array:
                        temp_func_name = f + "_"
                        if k.startswith(temp_func_name):
                            _col_name = k[len(temp_func_name):]
                            result.setdefault(_col_name, {})[f] = v
                return result
            else:
                return data

        columns = parse_columns(self, columns)

        # Ensure that is a list
        funcs = val_to_list(funcs)

        df = self

        # Parse the columns to float. Seems that spark can handle some aggregation with string columns giving
        # unexpected results
        # df = df.cols.cast(columns, "float")

        # Create a Column Expression for every column
        exprs = []
        for col_name in columns:
            for func in funcs:
                exprs.append(func(col_name).alias(func.__name__ + "_" + col_name))

        return (
            parse_col_names_funcs_to_keys(
                format_dict(df.agg(*exprs).to_json())
            )
        )

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
        for c in columns:
            max_val = self.cols.max(c)
            min_val = self.cols.min(c)
            range_result[c] = {'min': min_val, 'max': max_val}

        return range_result

    @add_attr(cols)
    # TODO: Use pandas or rdd for small datasets?!
    def median(columns):
        """
        Return the median of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns)

        return percentile(columns, [0.5])

    @add_attr(cols)
    def percentile(columns, values=None, error=1):
        """
        Return the percentile of a dataframe
        :param columns:  '*', list of columns names or a single column name.
        :param values: list of percentiles to be calculated
        :return: percentiles per columns
        """
        start_time = timeit.default_timer()

        if values is None:
            values = [0.05, 0.25, 0.5, 0.75, 0.95]

        columns = parse_columns(self, columns)

        # Get percentiles
        percentile_results = []
        for c in columns:
            percentile_per_col = self \
                .rows.drop_na(c) \
                .cols.cast(c, "double") \
                .approxQuantile(c, values, error)

            percentile_results.append(dict(zip(values, percentile_per_col)))

        percentile_results = dict(zip(columns, percentile_results))

        logging.info("percentile")
        logging.info(timeit.default_timer() - start_time)
        return format_dict(percentile_results)

    # Descriptive Analytics
    @add_attr(cols)
    # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
    def mad(col_name, more=None):
        """
        Return the Median Absolute Deviation
        :param col_name: Column to be processed
        :param more: Return some extra computed values (Median).
        :return:
        """

        # return mean(absolute(data - mean(data, axis)), axis)
        median_value = self.cols.median(col_name)

        mad_value = self.select(col_name) \
            .withColumn(col_name, F.abs(F.col(col_name) - median_value)) \
            .cols.median(col_name)

        if more:
            result = {"mad": mad_value, "median": median_value}
        else:
            result = mad_value

        return result

    @add_attr(cols)
    def std(columns):
        """
        Return the standard deviation of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        return _exprs(F.stddev, columns)

    @add_attr(cols)
    def kurt(columns):
        """
        Return the kurtosis of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        return _exprs(F.kurtosis, columns)

    @add_attr(cols)
    def mean(columns):
        """
        Return the mean of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        return _exprs(F.mean, columns)

    @add_attr(cols)
    def skewness(columns):
        """
        Return the skewness of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        return _exprs(F.skewness, columns)

    @add_attr(cols)
    def sum(columns):
        """
        Return the sum of a column dataframe
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        return _exprs(F.sum, columns)

    @add_attr(cols)
    def variance(columns):
        """
        Return the column variance
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        return _exprs(F.variance, columns)

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
            cnts = self.groupBy(col_name).count()
            mode_df = cnts.join(
                cnts.agg(F.max("count").alias("max_")), F.col("count") == F.col("max_")
            )

            # if none of the values are repeated we not have mode
            mode_list = (mode_df
                         .rows.select(mode_df["count"] > 1)
                         .cols.select(col_name)
                         .collect())

            mode_result.append({col_name: filter_list(mode_list)})

        return mode_result

    # String Operations
    @add_attr(cols)
    def lower(columns):
        """
        Lowercase all the string in a column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        def _lower(col, args):
            return F.lower(F.col(col))

        return apply_expr(columns, _lower, filter_col_by_dtypes="string")

    @add_attr(cols)
    def upper(columns):
        """
        Uppercase all the strings column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        def _upper(col, args):
            return F.upper(F.col(col))

        return apply_expr(columns, _upper, filter_col_by_dtypes="string")

    @add_attr(cols)
    def trim(columns):
        """
        Trim the string in a column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        def _trim(col_name, args):
            return F.trim(F.col(col_name))

        return apply_expr(columns, _trim)

    @add_attr(cols)
    def reverse(columns):
        """
        Reverse the order of all the string in a column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        def _reverse(col, args):
            return F.reverse(F.col(col))

        df = apply_expr(columns, _reverse, filter_col_by_dtypes="string")

        return df

    @add_attr(cols)
    def remove_accents(columns):
        """
        Remove accents in specific columns
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        columns = parse_columns(self, columns)

        def _remove_accents(col_name, attr):
            cell_str = col_name
            # first, normalize strings:
            nfkd_str = unicodedata.normalize('NFKD', cell_str)
            # Keep chars that has no other char combined (i.e. accents chars)
            with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
            return with_out_accents

        df = apply(columns, _remove_accents, "string")
        return df

    @add_attr(cols)
    def remove_special_chars(columns):
        """
        Reference https://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
        This method remove special characters (i.e. !”#$%&/()=?) in columns of dataFrames.
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        columns = parse_columns(self, columns)
        regex = re.compile("[%s]" % re.escape(string.punctuation))

        def _remove_special_chars(value, attr):
            return regex.sub("", value)

        df = apply(columns, _remove_special_chars, "string")
        return df

    @add_attr(cols)
    def remove_white_spaces(columns):
        """
        Remove all the white spaces from a string
        :param columns:
        :return:
        """
        columns = parse_columns(self, columns)

        def _remove_white_spaces(col_name, args):
            return F.regexp_replace(F.col(col_name), " ", "")

        df = apply_expr(columns, _remove_white_spaces)
        return df

    @add_attr(cols)
    def date_transform(col_name, new_col, current_format, output_format):
        """
        Tranform a column date format
        :param  col_name: Name date columns to be transformed. Columns ha
        :param  current_format: current_format is the current string dat format of columns specified. Of course,
                                all columns specified must have the same format. Otherwise the function is going
                                to return tons of null values because the transformations in the columns with
                                different formats will fail.
        :param  output_format: output date string format to be expected.
        """

        # Asserting if column if in dataFrame:
        validate_columns_names(self, col_name)

        def _date_transform(new_col, attr):
            _col_name = attr[0]
            _current_format = attr[1]
            _output_format = attr[2]
            return F.date_format(F.unix_timestamp(_col_name, _current_format).cast("timestamp"), _output_format).alias(
                new_col)

        return apply_expr(new_col, _date_transform, [col_name, current_format, output_format])

    @add_attr(cols)
    def years_between(col_name, new_col, date_format):
        """
        This method compute the age based on a born date.
        :param  col_name: Name of the column born dates column.
        :param  new_col: Name of the new column, the new columns is the resulting column of ages.
        :param  date_format: String format date of the column provided.


        """
        # Asserting if column if in dataFrame:
        validate_columns_names(self, col_name)

        # Output format date
        format_dt = "yyyy-MM-dd"  # Some SimpleDateFormat string

        def _years_between(new_col, attr):
            _date_format = attr[0]
            _col_name = attr[1]

            return F.format_number(
                F.abs(
                    F.months_between(
                        F.date_format(
                            F.unix_timestamp(
                                _col_name,
                                _date_format).cast("timestamp"),
                            format_dt),
                        F.current_date()) / 12), 4) \
                .alias(
                new_col)

        return apply_expr(new_col, _years_between, [date_format, col_name]).cols.cast(new_col, "float")

    @add_attr(cols)
    def impute(input_cols, output_cols, strategy="mean"):
        """
        Imputes missing data from specified columns using the mean or median.
        :param input_cols: List of columns to be analyze.
        :param output_cols: List of output columns with missing values imputed.
        :param strategy: String that specifies the way of computing missing data. Can be "mean" or "median"
        :return: Dataframe object (DF with columns that has the imputed values).
        """

        input_cols = parse_columns(self, input_cols)
        output_cols = val_to_list(output_cols)

        imputer = Imputer(inputCols=input_cols, outputCols=output_cols)

        df = self
        model = imputer.setStrategy(strategy).fit(df)
        df = model.transform(df)

        return df

    @add_attr(cols)
    def fill_na(columns, value):
        """
        Reaplce null data with a specified value
        :param columns:
        :param value:
        :return:
        """

        def _fill_na(col_name, args):
            return F.when(F.isnan(col_name) | F.col(col_name).isNull(), col_name).otherwise(args)

        return self.cols.apply_expr(columns, _fill_na, value)

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
        :param type: Accepts integer, float, string or None
        :return:
        """

        columns = parse_columns(self, columns)

        df = self
        expr = []
        for col_name in columns:
            # If type column is Struct parse to String. isnan/isNull can not handle Structure

            if is_(df.cols.schema_dtypes(col_name), (StructType, BooleanType)):
                df = df.cols.cast(col_name, "string")
            expr.append(F.count(F.when(F.isnan(col_name) | F.col(col_name).isNull(), col_name)).alias(col_name))

        result = format_dict(df.select(*expr).to_json())

        return result

    @add_attr(cols)
    def count_zeros(columns):
        """
        Return the NAN and Null count in a Column
        :param columns: '*', list of columns names or a single column name.
        :param type: Accepts integer, float, string or None
        :return:
        """
        columns = parse_columns(self, columns)
        df = self
        return format_dict(df.select([F.count(F.when(F.col(c) == 0, c)).alias(c) for c in columns]).to_json())

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
            result = {c: df.select(c).distinct().count() for c in columns}
        return result

    @add_attr(cols)
    def unique(columns):
        columns = parse_columns(self, columns)
        return self.select(columns).distinct()

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
    def _math(columns, operator):
        """
        Helper to process arithmetic operation between columns
        :param columns:
        :param operator:
        :return:
        """
        columns = parse_columns(self, columns, ["integer", "float"])
        assert len(columns) >= 2, "Error 2 or more columns needed"
        return self.select(reduce(operator, columns, 1))

    @add_attr(cols)
    def add(columns):
        """
        Add two or more columns
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        self._math(columns, lambda x, y: self[x] + self[y])

    @add_attr(cols)
    def sub(columns):
        """
        Subs two or more columns
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        self._math(columns, lambda x, y: self[x] - self[y])

    @add_attr(cols)
    def mul(columns):
        """
        Multiply two or more columns
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        self._math(columns, lambda x, y: self[x] * self[y])

    @add_attr(cols)
    def div(columns):
        """
        Divide two or more columns
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        self._math(columns, lambda x, y: self[x] / self[y])

    @add_attr(cols)
    def replace(columns, search_and_replace=None, value=None, regex=None):
        """
        Replace a value or a list of values by a specified string
        :param columns: '*', list of columns names or a single column name.
        :param search_and_replace: values to look at to be replaced
        :param value: new value to replace the old one
        :param regex:
        :return:
        """
        replace = None
        search = None

        if is_list_of_tuples(search_and_replace):
            params = list(zip(*search_and_replace))
            search = list(params[0])
            replace = list(params[1])

        elif is_list(search_and_replace):
            search = search_and_replace
            replace = value

        elif is_one_element(search_and_replace):
            search = val_to_list(search_and_replace)
            replace = value

        if regex:
            search = search_and_replace
            replace = value

        # if regex or normal replace we use regexp or replace functions
        # TODO check if .contains can be used instead of regexp
        def func_regex(_df, _col_name, _search, _replace):
            return _df.withColumn(c, F.regexp_replace(_col_name, _search, _replace))

        def func_replace(_df, _col_name, _search, _replace):
            data_type = self.cols.dtypes(_col_name)
            _search = [PYTHON_TYPES_[data_type](s) for s in _search]
            _df = _df.replace(_search, _replace, _col_name)
            return _df

        if regex:
            func = func_regex
        else:
            func = func_replace

        df = self

        columns = parse_columns(self, columns, filter_by_column_dtypes="string")
        for c in columns:
            df = func(df, c, search, replace)

        return df

    # Stats
    @add_attr(cols)
    def z_score(columns):
        """
        Return the column data type
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)

        df = self
        for c in columns:
            new_col = "z_col_" + c

            mean_value = self.cols.mean(columns)
            stdev_value = self.cols.std(columns)

            df = df.withColumn(new_col, F.abs((F.col(c) - mean_value) / stdev_value))
        return df

    @add_attr(cols)
    def iqr(columns, more=None):
        """
        Return the column data type
        :param columns:
        :param more: Return info about q1 and q3
        :return:
        """
        columns = parse_columns(self, columns)
        for c in columns:
            quartile = self.cols.percentile(c, [0.25, 0.75])
            q1 = quartile[0.25]
            q3 = quartile[0.75]

        iqr_value = q3 - q1
        if more:
            result = {"iqr": iqr_value, "q1": q1, "q3": q3}
        else:
            result = iqr_value
        return result

    @add_attr(cols)
    # TODO: Maybe we should create nest_to_vector and nest_array, nest_to_string
    def nest(input_cols, output_col, shape=None, separator=" "):
        """
        Concat multiple columns to one with the format specified
        :param input_cols: columns to be nested
        :param output_col: final column with the nested content
        :param separator: char to be used as separator at the concat time
        :param shape: final data type, 'array', 'string' or 'vector'
        :return: Spark DataFrame
        """
        columns = parse_columns(self, input_cols)
        df = self

        if shape is "vector":
            vector_assembler = VectorAssembler(
                inputCols=input_cols,
                outputCol=output_col)
            df = vector_assembler.transform(self)

        elif shape is "array":
            df = apply_expr(output_col, F.array(*columns))

        elif shape is "string":

            df = apply_expr(output_col, F.concat_ws(separator, *columns))
        else:
            RaiseIt.value_error(shape, ["vector", "array", "string"])

        return df

    @add_attr(cols)
    def unnest(columns, mark=None, n=None, index=None):
        """
        Split an array or string in different columns
        :param columns: Columns to be un-nested
        :param mark: is column is string
        :param n: Number of rows to un-nested
        :param index:
        :return: Spark DataFrame
        """

        # If a number of split was not defined try to infer the lenght with the first element
        infer_n = None
        if n is None:
            infer_n = True

        columns = parse_columns(self, columns)

        df = self

        for col_name in columns:
            # if the col is array
            expr = None

            col_dtype = self.schema[col_name].dataType

            # Array
            if is_(col_dtype, ArrayType):

                expr = F.col(col_name)
                # Try to infer the array length using the first row
                if infer_n is True:
                    n = len(self.cols.cell(col_name))

                for i in builtins.range(n):
                    df = df.withColumn(col_name + "_" + str(i), expr.getItem(i))

            # String
            elif is_(col_dtype, StringType):
                expr = F.split(F.col(col_name), mark)
                # Try to infer the array length using the first row
                if infer_n is True:
                    n = len(self.cols.cell(col_name).split(mark))

                if is_int(index):
                    r = builtins.range(index, index + 1)
                else:
                    r = builtins.range(0, n)

                for i in r:
                    df = df.withColumn(col_name + "_" + str(i), expr.getItem(i))

            # Vector
            elif is_(col_dtype, VectorUDT):
                def extract(row):
                    return row + tuple(row.vector.toArray().tolist())

                df = df.rdd.map(extract).toDF(df.columns)

        return df

    # TODO: Maybe we could merge this with un unnest. Like unnesting to the same column
    @add_attr(cols)
    def split(columns, mark):
        """
        A shortcut to the Apache Spark split
        :param columns: Column to be split
        :param mark: char used to split the column
        :return:
        """
        columns = parse_columns(self, columns)

        def _split(col_name, args):
            return F.split(F.col(col_name), mark)

        return apply_expr(columns, _split)

    @add_attr(cols)
    def cell(column):
        """
        Get the value for the first cell from a column in a data frame
        :param column: Column to be
        :return:
        """
        return self.cols.select(column).first()[0]

    @add_attr(cols)
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
            df = bucketizer(self, col_name, splits=splits)

            counts = (df.groupBy(col_name + "_buckets").agg(F.count(col_name + "_buckets").alias("count")).cols.rename(
                col_name + "_buckets", "value").sort(F.asc("value")).to_json())

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
                    hist_data.append({"count": 0, "lower": i[1]["lower"], "upper": i[1]["upper"]})
                elif "count" in i[0]:
                    hist_data.append({"count": i[0]["count"], "lower": i[1]["lower"], "upper": i[1]["upper"]})

        return hist_data

    @add_attr(cols)
    @dispatch((str, list), int)
    def hist(columns, buckets=10):
        return self.cols.hist(columns, fast_float(self.cols.min(columns)), fast_float(self.cols.max(columns)), buckets)

    @add_attr(cols)
    def frequency(columns, buckets=10):
        """
        Output values frequency in json format
        :param columns: Column to be processed
        :param buckets: Number of buckets
        :return:
        """
        columns = parse_columns(self, columns)
        df = self
        for col_name in columns:
            df = df.groupBy(col_name).count().rows.sort([("count", "desc"), (col_name, "desc")]).limit(
                buckets).cols.rename(col_name, "value")

        return df.to_json()

    @add_attr(cols)
    def schema_dtypes(columns):
        """
        Return the columns data type as Type
        :param columns:
        :return:
        """
        columns = parse_columns(self, columns)
        return format_dict([self.schema[col_name].dataType for col_name in columns])

    @add_attr(cols)
    def dtypes(columns):
        """
        Return the column data type as string
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)
        data_types = tuple_to_dict(self.dtypes)

        return format_dict({c: data_types[c] for c in columns})

    @add_attr(cols)
    def qcut(input_col, output_col, num_buckets):
        """
        Bin columns into n buckets
        :param input_col:
        :param output_col:
        :param num_buckets:
        :return:
        """
        discretizer = QuantileDiscretizer(numBuckets=num_buckets, inputCol=input_col, outputCol=output_col)
        return discretizer.fit(self).transform(self)

    @add_attr(cols)
    def clip(columns, lower, upper):
        """
        Trim values at input thresholds
        :param columns: Columns to be trimmed
        :param lower: Lower
        :param upper:
        :return:
        """

        columns = parse_columns(self, columns)

        def _clip(_col_name, args):
            _lower = args[0]
            _upper = args[1]
            return (F.when(F.col(_col_name) <= _lower, _lower)
                    .when(F.col(_col_name) >= _upper, _upper)).otherwise(F.col(col_name))

        df = self
        for col_name in columns:
            df = df.cols.apply_expr(col_name, _clip, [lower, upper])
        return df

    @add_attr(cols)
    def abs(columns):
        """
        Apply abs to the values in a column
        :param columns:
        :return:
        """
        columns = parse_columns(self, columns)
        df = self
        for col_name in columns:
            df = df.withColumn(col_name, F.abs(F.col(col_name)))
        return df

    return cols


DataFrame.cols = property(cols)
