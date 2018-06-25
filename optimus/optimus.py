# Used in decorators. This convenience func preserves name and docstring
from functools import wraps

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import unicodedata
from pyspark.sql.functions import col, udf, trim, lit, format_number, months_between, date_format, unix_timestamp, \
    current_date, abs as mag

from pyspark.sql.types import StructType, StructField

from optimus.spark import *

from optimus.assertion_helpers import *


def create_df(rows_data, col_specs):
    """
    Helper to create a Spark dataframe
    :param rows_data:
    :param col_specs:
    :return:
    """
    struct_fields = list(map(lambda x: StructField(*x), col_specs))
    return get_spark().createDataFrame(rows_data, StructType(struct_fields))


class Optimus:
    """

    """

    def __init__(self):
        print("init")

    def query(self):
        """
        Select row depende
        :return:
        """
        # https://stackoverflow.com/questions/11869910/pandas-filter-rows-of-dataframe-with-operator-chaining
        print("hola")

    def lookup(self, columns, look_up_key=None, replace_by=None):
        """

        :param column:
        :param look_up_key: This is the list
        :param replace_by:
        :return:
        """

        # Check that columns are valid
        columns = self._assert_columns_names(columns)

        # Asserting columns is string or list:
        assert isinstance(replace_by, (str, dict)), "Error: str_to_replace argument must be a string or a dict"

        if isinstance(replace_by, dict):
            assert (replace_by != {}), "Error, str_to_replace must be a string or a non empty python dictionary"
            assert (
                    look_up_key is None), "Error, If a python dictionary if specified, list_str argument must be None: list_str=None"

    def move_col(self, column, ref_col, position):
        """
        This function change column position in dataFrame.
        :param column:
        :param ref_col:
        :param position:
        :return:
        """

        # Check that column is a string or a list
        column = self._assert_columns_names(column)
        assert len(column) == 1, "Error: Columns must be a string or a list of one element"

        # Check if columns argument a string datatype:
        ref_col = self._assert_columns_names(ref_col)

        # Asserting if position is 'after' or 'before'
        assert (position == 'after') or (
                position == 'before'), "Error: Position parameter only can be 'after' or 'before' actually" % position

        # Get dataframe columns
        columns = self.columns

        # Finding position of column to move:
        find_col = lambda columns, column: [index for index, c in enumerate(columns) if c == column]
        new_index = find_col(columns, ref_col)
        old_index = find_col(columns, column)

        # if position is 'after':
        if position == 'after':
            # Check if the movement is from right to left:
            if new_index[0] >= old_index[0]:
                columns.insert(new_index[0], columns.pop(old_index[0]))  # insert and delete a element
            else:  # the movement is form left to right:
                columns.insert(new_index[0] + 1, columns.pop(old_index[0]))
        else:  # If position if before:
            if new_index[0] >= old_index[0]:  # Check if the movement if from right to left:
                columns.insert(new_index[0] - 1, columns.pop(old_index[0]))
            elif new_index[0] < old_index[0]:  # Check if the movement if from left to right:
                columns.insert(new_index[0], columns.pop(old_index[0]))

        return self[columns]

    # Not sure if this function is helpfull
    def count_items(self, col_id, col_search, new_col_feature, search_string):
        print(1)

    def age_calculate(self, column, dates_format, name_col_age):
        """
        This method compute the age of clients based on their born dates.
        :param  column      Name of the column born dates column.
        :param  dates_format  String format date of the column provided.
        :param  name_col_age  Name of the new column, the new columns is the resulting column of ages.

        """
        # Check if column argument a string datatype:
        column = self._assert_columns_names(column)
        assert len(column) == 1, "Error: Columns must be a string or a list of one element"

        # Check if dates_format argument a string datatype:
        self._assert_type_str(dates_format, "dates_format")

        # Asserting if column if in dataFrame:
        name_col_age = self._assert_columns_names(name_col_age)
        assert len(name_col_age) == 1, "Error: Columns must be a string or a list of one element"

        # Output format date
        format_dt = "yyyy-MM-dd"  # Some SimpleDateFormat string

        exprs = format_number(
            mag(
                months_between(date_format(
                    unix_timestamp(column, dates_format).cast("timestamp"), format_dt), current_date()) / 12), 4).alias(
            name_col_age)

        return self.withColumn(name_col_age, exprs)

    def astype(self, cols_and_types):
        """
        Cast columns to a specific type
        :param cols_and_types: List of tuples of column names and types to be casted. This variable should have the
                following structure:

                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]

                The first parameter in each tuple is the column name, the second is the final datatype of column after
                the transformation is made.
        :return:
        """

        dict_types = {'string': StringType(), 'str': StringType(), 'integer': IntegerType(),
                      'int': IntegerType(), 'float': FloatType(), 'double': DoubleType(), 'Double': DoubleType()}

        types = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int',
                 'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}

        # Asserting cols_and_types is string or list:
        columns = self._check_columns_tuples(cols_and_types)

        not_specified_columns = filter(lambda c: c not in columns, self.columns)

        exprs = [col(column[0]).cast(dict_types[types[column[1]]]).alias(column[0]) for column in cols_and_types] + [
            col(column) for column in not_specified_columns]

        return self.select(*exprs)

    ## Not sure about this
    def empty_str_to_str(self, columns, custom_str):
        print(1)


# Reference https://medium.com/@mgarod/dynamically-add-a-method-to-a-class-in-python-c49204b85bd6
# Decorator to attach a custom functions to a class


def add_method(cls):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        setattr(cls, func.__name__, wrapper)
        # Note we are not binding func, but wrapper which accepts self but does exactly the same as func
        return func  # returning func means func can still be used normally

    return decorator


@add_method(DataFrame)
def _parse_columns(self, columns, index=None):
    """
    Check that a column list is a valis list of columns.
    :param columns:  Acepts * as param to return all the string columns in the dataframe
    :return: A list of columns string names
    """

    # Verify that columns are a string or list of string
    assert_type_str_or_list(columns)

    # if columns value is * get all dataframes columns
    if columns == "*":
        columns = list(map(lambda t: t[0], self.dtypes))

    # if string convert to list. Because we always return a list

    if isinstance(columns, str):
        columns = [columns]

    # Verify if we have a list
    elif isinstance(columns, list):
        # Verify that we have list inside the tuples
        if all(isinstance(x, tuple) for x in columns):
            # Extract a specific position in the tupple
            columns = [c[index] for c in columns]

    # Validate that all the columns exist
    validate_columns_names(self, columns)

    return columns


@add_method(DataFrame)
def lower(self, columns):
    """
    Lowercase all the string in a column
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, F.lower)


@add_method(DataFrame)
def upper(self, columns):
    """
    Uppercase all the strings column
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, F.upper)


@add_method(DataFrame)
def trim(self, columns):
    """
    Trim the string in a column
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, F.trim)


@add_method(DataFrame)
def reverse(self, columns):
    """
    Reverse the order of all the string in a column
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, F.reverse)


@add_method(DataFrame)
def apply_to_row(self, columns, func):
    """
    Apply the func function to a serie of row in specific columns
    :param columns:
    :param func:
    :return:
    """

    columns = self._parse_columns(columns)

    for column in columns:
        self = self.withColumn(column, func(col(column)))
    return self


@add_method(DataFrame)
# FIX: We must a find the best approach to fix the collition between the spark dataframe and optimus
# drop method and the optimus methods
def drop_(self, columns):
    """

    :param columns: *, string or string or columns list to be dropped
    :return: Dataframe
    """
    columns = self._parse_columns(columns)
    for column in columns:
        self = self.drop(column)
    return self


@add_method(DataFrame)
def keep(self, columns):
    """
    Just Keep the columns and drop.
    :param columns:
    :return:
    """

    columns = self._parse_columns(columns)
    return self.select(*columns)


@add_method(DataFrame)
def rename(self, columns_pair):
    """"
    This functions change the name of a column(s) datraFrame.
    :param columns_pair: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
    """
    # Check that the 1st element in the tuple is a valis set of columns
    columns = self._parse_columns(columns_pair, 0)

    # Rename cols
    columns = [col(column[0]).alias(column[1]) for column in columns_pair]

    return self.select(columns)


def _remove_accents(input_str):
    """
    Remove accents to a string
    :return:
    """
    # first, normalize strings:

    nfkd_str = unicodedata.normalize('NFKD', input_str)

    # Keep chars that has no other char combined (i.e. accents chars)
    with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])

    return with_out_accents

@add_method(DataFrame)
def remove_accents(self, columns):
    """
    Remove accents in specific columns
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, _remove_accents)

# Quantile statistics
@add_method(DataFrame)
def _agg(self, agg, columns):
    """
    Helper function to manage aggregation functions
    :param agg: Aggregation function from spark
    :param columns: list of columns names or a string (a column name).
    :return:
    """
    columns = self._parse_columns(columns)

    # Return the min value
    r = list(map(lambda c: self.agg({c: agg}).collect()[0][0], columns))

    # if the list has one elment return just a single element
    return one_list_to_val(r)


@add_method(DataFrame)
def min(self, columns):
    """
    Return the min value from a column dataframe
    :param columns: '*', list of columns names or a string (a column name).
    :return:
    """
    return self._agg("min", columns)


@add_method(DataFrame)
def max(self, columns):
    """
    Return the max value from a column dataframe
    :param columns: '*', list of columns names or a string (a column name).
    :return:
    """
    return self._agg("max", columns)


@add_method(DataFrame)
def range(self, columns):
    """
    Return the range form the min to the max value
    :param columns:
    :return:
    """

    columns = self._parse_columns(columns)

    # if max_val, min_val has 1 element convert to a list
    max_val = val_to_list(self.max(columns))
    min_val = val_to_list(self.min(columns))

    # Substract max and min lists to get the range
    return one_list_to_val([x - y for x, y in zip(max_val, min_val)])

@add_method(DataFrame)
def median(self, columns):
    """
    Return the median of a column dataframe
    :param columns:
    :return:
    """

    return self.approxQuantile(columns, [0.5], 0)[0]


# Descriptive Analytics
@add_method(DataFrame)
def stddev(self, columns):
    """
    Return the standard deviation of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("stddev", columns)


@add_method(DataFrame)
def kurt(self, columns):
    """
    Return the kurtosis of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("kurtosis", columns)


@add_method(DataFrame)
def mean(self, columns):
    """
    Return the mean of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("mean", columns)


@add_method(DataFrame)
def skewness(self, columns):
    """
    Return the skewness of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("skewness", columns)


@add_method(DataFrame)
def sum(self, columns):
    """
    Return the sum of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("skewness", columns)


@add_method(DataFrame)
def variance(self, columns):
    """
    Return the variance of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("variance", columns)
