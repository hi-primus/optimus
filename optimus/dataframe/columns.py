import unicodedata
import string
import re
from functools import reduce
import builtins

from multipledispatch import dispatch

from pyspark.ml.feature import Imputer
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import Column

from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.utils import AnalysisException

# Helpers
from optimus.helpers.constants import *
from optimus.helpers.decorators import add_attr, add_method
from optimus.helpers.checkit \
    import is_str, is_num_or_str, is_list, is_, is_tuple, is_list_of_dataframes, is_list_of_tuples, \
    is_function, is_one_element, is_same_class, is_type, is_int, cast_to_float

from optimus.helpers.functions \
    import validate_columns_names, parse_columns, parse_spark_dtypes, collect_to_dict, format_dict, \
    tuple_to_dict, val_to_list, filter_list, one_list_to_val

from optimus.functions import filter_row_by_data_type as fbdt
from optimus.functions import abstract_udf as audf, concat

from optimus.helpers.raiseit import RaiseIfNot


@add_method(DataFrame)
def cols(self):
    @add_attr(cols)
    @dispatch(str, object)
    def append(col_name=None, value=None):
        """
        Append a Column to a Dataframe
        :param col_name:
        :param value:
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
        :param cols_values:
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
                df_result = df_result.cols().append(col_name, value)

        return df_result

    @add_attr(cols)
    def filter(columns=None, regex=None, data_type=None):
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
    def apply_exp(columns, func=None, attrs=None, col_exp=None, filter_col_by_dtypes=None, verbose=True):
        """
        :param columns: Columns in which the columns are going to be applied
        :param func:
        :param column_data_type:
        :param  attrs:
        :return:
        """

        def func_col_exp(col_name, attr):
            return func

        if is_(func, Column):
            _func = func_col_exp
        else:
            _func = func

        columns = parse_columns(self, columns, filter_by_column_dtypes=filter_col_by_dtypes, accepts_missing_cols=True)

        # if columns do not exits
        if not columns:
            columns = val_to_list(columns)

        df = self
        for col_name in columns:
            df = df.withColumn(col_name, audf(col_name, _func, attrs=attrs, func_type="column_exp", verbose=verbose))
        return df

    @add_attr(cols)
    def apply(columns, func, func_return_type, args=None, func_type=None, when=None, filter_col_by_dtypes=None,
              verbose=True):
        """
        Apply a function using pandas udf or udf if apache arrow is not available
        :param columns:
        :param func: Functions to be applied to a columns
        :param func_return_type
        :param args:
        :param func_type: pandas_udf or udf. If none try to use pandas udf (Pyarrow needed)
        :param when:
        :param filter_col_by_dtypes:
        :return:
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
        :param columns:
        :param func: Functions to be applied to a columns
        :param func_return_type
        :param args:
        :param func_type: pandas_udf or udf. If none try to use pandas udf (Pyarrow needed)
        :param data_type:
        :return:
        """
        columns = parse_columns(self, columns)

        for c in columns:
            df = self.cols().apply(c, func, func_return_type, args=args, func_type=func_type,
                                   when=fbdt(c, data_type))
        return df

    @add_attr(cols)
    def rename(columns_old_new=None, func=None):
        """"
        This functions change the name of a column(s) dataFrame.
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
                df = df.withColumnRenamed(c[0], c[1])

        return df

    @add_attr(cols)
    @dispatch(list)
    def cast(cols_and_dtypes):
        """
        Cast multiple columns to a specific datatype
        List of tuples of column names and types to be casted. This variable should have the
                following structure:

                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]

                The first parameter in each tuple is the column name, the second is the final datatype of column after
                the transformation is made.

        :param cols_and_dtypes: Columns to be casted and new data types
        :return:
        """
        cols, attrs = parse_columns(self, cols_and_dtypes, get_args=True)
        return _cast(cols, attrs)

    @add_attr(cols)
    @dispatch((list, str), object)
    def cast(columns, dtypes):
        """
        Cast a column or a list of columns to a specific datatype
        :param columns: Columns to be casted
        :param dtypes: final data type
        :return:
        """
        cols = parse_columns(self, columns)
        attrs = []
        for _ in builtins.range(0, len(cols)):
            attrs.append((dtypes,))

        return _cast(cols, attrs)

    @add_attr(cols)
    def _cast(cols, attrs):
        """
        Helper function to support the multiple params implementation
        :param cols:
        :param attrs:
        :return:
        """

        # assert validate_columns_names(self, cols_and_types, 0)
        # cols, attrs = parse_columns(self, cols_and_dtypes, get_args=True)

        # if parse_spark_dtypes(attr[0])
        def cast_factory(cls):

            # Parse standard data types
            if parse_spark_dtypes(cls):
                func_type = "column_exp"

                def cast_to_vectors(col_name, attr):
                    return F.col(col_name).cast(parse_spark_dtypes(cls))

                func_return_type = None

            # Parse to Vector
            elif is_type(cls, Vectors):
                func_type = "udf"

                def cast_to_vectors(val, attr):
                    return Vectors.dense(val)

                func_return_type = VectorUDT()

            # Add here any other parse you want
            else:
                RaiseIfNot.value_error(cls)

            return func_return_type, cast_to_vectors, func_type

        df = self
        for col, args in zip(cols, attrs):
            return_type, func, func_type = cast_factory(args[0])
            df = df.withColumn(col, audf(col, func,
                                         func_return_type=return_type,
                                         attrs=args[0],
                                         func_type=func_type, verbose=False)
                               )
        return df

    @add_attr(cols)
    def astype(*args, **kwargs):
        return cast(*args, **kwargs)

    @add_attr(cols)
    def move(column, ref_col, position):
        """
        Move a column to specific position
        :param column: Column to be moved
        :param ref_col: Column taken as reference
        :param position:
        :return:
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
        Just Keep the columns and drop.
        :param columns:
        :param regex:
        :return:
        """

        if regex:
            r = re.compile(regex)
            columns = list(filter(r.match, self.columns))

        columns = parse_columns(self, columns)
        return self.select(*columns)

    @add_attr(cols)
    # TODO: Create a function to sort by datatype?
    def sort(order="asc"):
        """
        Sort dataframes columns asc or desc
        :param order: Apache Spark Dataframe
        :return:
        """

        if order == "asc":
            sorted_col_names = sorted(self.columns)
        elif order == "desc":
            sorted_col_names = sorted(self.columns, reverse=True)
        else:
            RaiseIfNot.value_error(order, ["asc", "desc"])

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
            columns = list(filter(r.match, self.columns))

        columns = parse_columns(self, columns, filter_by_column_dtypes=data_type)

        for column in columns:
            df = df.drop(column)
        return df

    @add_attr(cols)
    def _exprs(agg, columns):
        """
        Helper function to manage aggregation functions
        :param agg: Aggregation function from Apache Spark
        :param columns: list of columns names or a string (a column name).
        :return:
        """
        # Ensure that is a list
        agg = val_to_list(agg)

        # Filter only numeric columns
        columns = parse_columns(self, columns)

        # Aggregate
        df = self
        exprs = []
        for c in columns:
            for a in agg:
                exprs.append(a(c).alias(a.__name__ + "_" + c))

        return format_dict(collect_to_dict(df.agg(*exprs).collect()))

    # Quantile statistics
    @add_attr(cols)
    def min(columns):
        """
        Return the min value from a column dataframe
        :param columns: '*', list of columns names or a string (a column name).
        :return:
        """
        return _exprs(F.min, columns)

    @add_attr(cols)
    def max(columns):
        """
        Return the max value from a column dataframe
        :param columns: '*', list of columns names or a string (a column name).
        :return:
        """
        return _exprs("max", columns)

    @add_attr(cols)
    def range(columns):
        """
        Return the range form the min to the max value
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)

        range = {}
        for c in columns:
            max_val = self.cols().max(c)
            min_val = self.cols().min(c)
            range[c] = {'min': min_val, 'max': max_val}

        return range

    @add_attr(cols)
    # TODO: Use pandas or rdd for small datasets?!
    def median(columns):
        """
        Return the median of a column dataframe
        :param columns:
        :return:
        """
        columns = parse_columns(self, columns)

        return percentile(columns, [0.5])

    @add_attr(cols)
    def percentile(columns, percentile=[0.05, 0.25, 0.5, 0.75, 0.95], error=0):
        """
        Return the percentile of a dataframe
        :param columns: 
        :param percentile:
        :return: 
        """

        columns = parse_columns(self, columns)

        # Get percentiles
        percentile_value = []
        for c in columns:
            percentile_results = self \
                .rows().drop_na(c) \
                .cols().cast((c, "double",)) \
                .approxQuantile(c, percentile, error)

            percentile_value.append(dict(zip(percentile, percentile_results)))

        percentile_value = dict(zip(columns, percentile_value))

        return format_dict(percentile_value)

        # Merge percentile and value
        # percentile_value = list(map(lambda r: dict(zip(percentile, r)), percentile_results))

        # Merge percentile, values and columns
        return format_dict(dict(zip(columns, percentile_value)))

    # Descriptive Analytics

    @add_attr(cols)
    # TODO: implemente double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
    def mad(col_name, more=None):
        """
        Return the Median Absolute Deviation
        :param col_name: Column to be processed
        :param more: Return some extra computed values (Median).
        :return:
        """

        # return mean(absolute(data - mean(data, axis)), axis)
        median_value = self.cols().median(col_name)

        mad_value = self.select(col_name) \
            .withColumn(col_name, F.abs(F.col(col_name) - median_value)) \
            .cols().median(col_name)

        if more:
            result = {"mad": mad_value, "median": median_value}
        else:
            result = mad_value

        return result

    @add_attr(cols)
    def std(columns):
        """
        Return the standard deviation of a column dataframe
        :param columns:
        :return:
        """
        return _exprs("stddev", columns)

    @add_attr(cols)
    def kurt(columns):
        """
        Return the kurtosis of a column dataframe
        :param columns:
        :return:
        """
        return _exprs("kurtosis", columns)

    @add_attr(cols)
    def mean(columns):
        """
        Return the mean of a column dataframe
        :param columns:
        :return:
        """
        return _exprs("mean", columns)

    @add_attr(cols)
    def skewness(columns):
        """
        Return the skewness of a column dataframe
        :param columns:
        :return:
        """
        return _exprs("skewness", columns)

    @add_attr(cols)
    def sum(columns):
        """
        Return the sum of a column dataframe
        :param columns:
        :return:
        """
        return _exprs("sum", columns)

    @add_attr(cols)
    def variance(columns):
        """
        Return the column variance
        :param columns:
        :return:
        """
        return _exprs("variance", columns)

    @add_attr(cols)
    def mode(columns):
        """
        Return the the column mode
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)
        mode_result = []

        for c in columns:
            cnts = self.groupBy(c).count()
            mode_df = cnts.join(
                cnts.agg(F.max("count").alias("max_")), F.col("count") == F.col("max_")
            )

            # if none of the values are repeated we not have mode
            mode_list = mode_df.filter(mode_df["count"] > 1).select(c).collect()

            mode_result.append({c: filter_list(mode_list)})

        return mode_result

    # String Operations
    @add_attr(cols)
    def lower(columns):
        """
        Lowercase all the string in a column
        :param columns:
        :return:
        """

        def _lower(col, args):
            return F.lower(F.col(col))

        return apply_exp(columns, _lower, filter_col_by_dtypes="string")

    @add_attr(cols)
    def upper(columns):
        """
        Uppercase all the strings column
        :param columns:
        :return:
        """

        def _upper(col, args):
            return F.upper(F.col(col))

        return apply_exp(columns, _upper, filter_col_by_dtypes="string")

    @add_attr(cols)
    def trim(columns):
        """
        Trim the string in a column
        :param columns:
        :return:
        """

        def _trim(col, args):
            return F.trim(F.col(col))

        return apply_exp(columns, _trim)

    @add_attr(cols)
    def reverse(columns):
        """
        Reverse the order of all the string in a column
        :param columns:
        :return:
        """

        def _reverse(col, args):
            return F.reverse(F.col(col))

        return apply_exp(columns, _reverse, filter_col_by_dtypes="string")

    @add_attr(cols)
    def remove_accents(columns):
        """
        Remove accents in specific columns
        :param columns:
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

        df = apply(columns, _remove_accents, "str")
        return df

    @add_attr(cols)
    def remove_special_chars(columns):
        """
        Remove accents in specific columns
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)

        def _remove_special_chars(col_name, attr):
            # Remove all punctuation and control characters
            for punct in (set(col_name) & set(string.punctuation)):
                col_name = col_name.replace(punct, "")
            return col_name

        df = apply(columns, _remove_special_chars, "str")
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
        # Check if current_format argument a string datatype:
        assert isinstance(current_format, str)

        # Check if output_format argument a string datatype:
        assert isinstance(output_format, str)

        # Asserting if column if in dataFrame:
        validate_columns_names(self, col_name)

        def _date_transform(col_name, attr):
            new_col = attr[0]
            current_format = attr[1]
            output_format = attr[2]
            return F.date_format(F.unix_timestamp(col_name, current_format).cast("timestamp"), output_format).alias(
                new_col)

        return apply_exp(col_name, _date_transform, [new_col, current_format, output_format])

    @add_attr(cols)
    def years_between(col_name, new_col, format_date):
        """
        This method compute the age based on a born date.
        :param  new_col: Name of the new column, the new columns is the resulting column of ages.
        :param  format_date: String format date of the column provided.
        :param  col_name: Name of the column born dates column.

        """
        # Check if column argument a string datatype:
        assert isinstance(format_date, str)

        # Check if dates_format argument a string datatype:
        assert isinstance(new_col, str)

        # Asserting if column if in dataFrame:
        validate_columns_names(self, col_name)

        # Output format date
        format_dt = "yyyy-MM-dd"  # Some SimpleDateFormat string

        def _years_between(name_col_age, attr):
            dates_format = attr[0]
            col_name = attr[1]

            return F.format_number(
                F.abs(
                    F.months_between(
                        F.date_format(
                            F.unix_timestamp(
                                col_name,
                                dates_format).cast("timestamp"),
                            format_dt),
                        F.current_date()) / 12), 4) \
                .alias(
                name_col_age)

        return apply_exp(new_col, _years_between, ["yyyyMMdd", col_name])

    @add_attr(cols)
    def impute(columns, out_cols, strategy):
        """
        Imputes missing data from specified columns using the mean or median.
        :param columns: List of columns to be analyze.
        :param out_cols: List of output columns with missing values imputed.
        :param strategy: String that specifies the way of computing missing data. Can be "mean" or "median"
        :return: Transformer object (DF with columns that has the imputed values).
        """

        # Check if columns to be process are in dataframe
        # TODO: this should filter only numeric values
        columns = parse_columns(self, columns)

        assert isinstance(out_cols, list), "Error: out_cols argument must be a list"

        assert (strategy == "mean" or strategy == "median"), "Error: strategy has to be 'mean' or 'median'."

        imputer = Imputer(inputCols=columns, outputCols=out_cols)

        df = self
        model = imputer.setStrategy(strategy).fit(df)
        df = model.transform(df)

        return df

    @add_attr(cols)
    def show(columns):
        columns = parse_columns(self, columns)
        print(self.cols().schema_dtypes(columns))
        self.select("antiguedad_construccion").show(100)

    @add_attr(cols)
    def count_na(columns):
        """
        Return the NAN and Null count in a Column
        :param columns:
        :param type: Accepts integer, float, string or None
        :return:
        """

        columns = parse_columns(self, columns)
        df = self
        # df = df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in columns]) Just count Nans

        expr = []
        for c in columns:
            # If type column is Struct parse to String. isnan/isNull can not handle Structure

            if is_(df.cols().schema_dtypes(c), (StructType, BooleanType)):
                df = df.cols().cast(c, "string")
            expr.append(F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c))

        # print(df)
        result = collect_to_dict(df.select(*expr).collect())
        # except AnalysisException:
        #    pass

        return result

    @add_attr(cols)
    def count_zeros(columns):
        """
        Return the NAN and Null count in a Column
        :param columns:
        :param type: Accepts integer, float, string or None
        :return:
        """
        columns = parse_columns(self, columns)
        df = self
        return format_dict(collect_to_dict(df.select([F.count(F.when(F.col(c) == 0, c)).alias(c) for c in columns]) \
                                           .collect()))

    # TODO: Explorer the use of approxCountDistinct
    @add_attr(cols)
    def count_uniques(columns, estimate=True):
        """
        Return how many unique items exist in a columns
        :param columns:
        :param estimate:
        :return:
        """
        columns = parse_columns(self, columns)

        if estimate is True:
            resutl = _exprs(F.approx_count_distinct, columns)
        else:
            df = self
            resutl = {c: df.select(c).distinct().count() for c in columns}
        return resutl

    @add_attr(cols)
    def filter_by_dtypes(data_type):
        """
        This function returns column of dataFrame which have the same
        datatype provided. It gets the column datatype from dataFrame.dtypes method.

        :param data_type:
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
        :param columns:
        :return:
        """

        self._math(columns, lambda x, y: self[x] + self[y])

    @add_attr(cols)
    def sub(columns):
        """
        Subs two or more columns
        :param columns:
        :return:
        """
        self._math(columns, lambda x, y: self[x] - self[y])

    @add_attr(cols)
    def mul(columns):
        """
        Multiply two or more columns
        :param columns:
        :return:
        """
        self._math(columns, lambda x, y: self[x] * self[y])

    @add_attr(cols)
    def div(columns):
        """
        Divide two or more columns
        :param columns:
        :return:
        """
        self._math(columns, lambda x, y: self[x] / self[y])

    @add_attr(cols)
    def replace(columns, search_and_replace=None, value=None, regex=None):
        """
        Replace
        :param columns:
        :param search_and_replace: values to look at to be replaced
        :param value:
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
            data_type = self.cols().dtypes(_col_name)
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

    @add_attr(cols)
    def dtypes(columns):
        """
        Return the column data type
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)
        data_types = tuple_to_dict(self.dtypes)

        return format_dict({c: data_types[c] for c in columns})

    # Stats
    @add_attr(cols)
    def z_score(columns, new_col=None):
        """
        Return the column data type
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)

        df = self
        for c in columns:
            new_col = "z_col_" + c

            mean_value = self.cols().mean(columns)
            stdev_value = self.cols().std(columns)

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
            quartile = self.cols().percentile(c, [0.25, 0.75])
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
    def nest(input_cols, output_cols, separator=" ", shape=None):
        """
        Concat multiple columns to one with the format specified
        :param input_cols:
        :param output_cols:
        :param separator:
        :param shape: a string with
        :return:
        """
        columns = parse_columns(self, input_cols)
        df = self

        if shape is "vector":
            vector_assembler = VectorAssembler(
                inputCols=input_cols,
                outputCol=output_cols)
            df = vector_assembler.transform(self)

        elif shape is "array":
            df = apply_exp(output_cols, F.array(*columns))

        elif shape is "string":
            df = apply_exp(output_cols, F.concat_ws(separator, *columns))
        else:
            RaiseIfNot.value_error(shape, ["vector", "array", "string"])

        return df

    @add_attr(cols)
    def cell(column):
        """
        Get the value from one cell in a data frame
        :param column:
        :return:
        """
        return self.cols().filter(column).first()[0]

    @add_attr(cols)
    def unnest(columns, mark=None, n=None, index=None):
        """
        Split array or string in different columns
        :param columns:
        :param n:
        :return:
        """

        flag_n = None
        if n is None:
            flag_n = True

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
                if flag_n is True:
                    n = len(self.cols().cell(col_name))

                for i in builtins.range(n):
                    df = df.withColumn(col_name + "_" + str(i), expr.getItem(i))

            # String
            elif is_(col_dtype, StringType):
                expr = F.split(F.col(col_name), mark)
                # Try to infer the array length using the first row
                if flag_n is True:
                    n = len(self.cols().cell(col_name).split(mark))

                if is_int(index):
                    r = builtins.range(index, index + 1)
                else:
                    r = builtins.range(0, n)

                for i in r:
                    df = df.withColumn(col_name + "_" + str(i), expr.getItem(i))

            # Vectorp
            elif is_(col_dtype, VectorUDT):
                def extract(row):
                    return row + tuple(row.vector.toArray().tolist())

                df = df.rdd.map(extract).toDF(df.columns)

        return df

    @add_method(cols)
    def _hist(columns, bins=10):
        """

        :param column:
        :param bins:
        :return:
        """

        # Quantile Discretizer needs a numeric columns
        df = self.cols().cast(columns, "double")

        for col_name in columns:
            temp_col = "bucket_" + col_name
            discretizer = QuantileDiscretizer(numBuckets=bins, inputCol=col_name, outputCol=temp_col)

            df = discretizer.fit(df).transform(df)

            df = df.groupBy(temp_col).agg(F.min(col_name).alias('min' + '_' + col_name),
                                          F.max(col_name).alias('max' + '_' + col_name),
                                          F.count(temp_col).alias('count' + "_" + col_name)).orderBy(temp_col)
        return df

    @add_method(cols)
    def hist(columns, bins=10):
        """

        :param columns:
        :param bins:
        :return:
        """
        columns = parse_columns(self, columns)
        return format_dict(collect_to_dict(self.cols()._hist(columns, bins).collect()))

    @add_method(cols)
    def schema_dtypes(columns):
        """

        :param columns:
        :return:
        """
        columns = parse_columns(self, columns)
        return format_dict([self.schema[col_name].dataType for col_name in columns])

    return cols
