from pyspark.sql import DataFrame
from pyspark.sql.dataframe import *

from pyspark.sql import functions as F

import re

# Library used for method overloading using decorators
from multipledispatch import dispatch

# Helpers
from optimus.helpers.validators import *
from optimus.helpers.constants import *
from optimus.helpers.decorators import *

from pyspark.sql.functions import Column

import builtins


@add_method(DataFrame)
def cols(self):
    @add_attr(cols)
    @dispatch(str, object)
    def create(name=None, value=None):
        """

        :param name:
        :param value:
        :return:
        """

        assert isinstance(name, str), "name param must be a string"
        assert isinstance(value, (int, float, str, Column)), "object param must be a number, str or a Column "

        if isinstance(value, (int, str, float)):
            value = F.lit(value)

        return self.withColumn(name, value)

    @add_attr(cols)
    @dispatch(list)
    def create(col_names=None):
        """

        :param col_names:
        :return:
        """
        df = self
        for c in col_names:
            value = c[1]
            name = c[0]

            # Create a column if necessary
            if isinstance(value, (int, str, float)):
                value = F.lit(value)
            df = df.withColumn(name, value)
        return df

    @add_attr(cols)
    def select(columns=None, regex=None):
        """
        SElect columns using index or column name
        :param columns:
        :param regex:
        :return:
        """

        if regex:
            r = re.compile(regex)
            columns = list(filter(r.match, self.columns))

        def get_column(column):
            if isinstance(column, int):
                col_name = self.columns[column]
            elif isinstance(column, str):
                col_name = column
            return col_name

        return self.select(list(map(get_column, columns)))

    @add_attr(cols)
    def apply(col_and_attr, func, col_as_params=False):
        """
        Operation as rename or cast need to reconstruct the columns with new types or names.
        This is a helper function handle it
        :param col_and_attr:
        :param func:
        :param col_as_params: send all the col_and_attr info to the function
        :return:
        """

        if col_as_params:
            params = col_and_attr[0]
        else:
            params = parse_columns(self, col_and_attr)

        df = self
        for param in params:
            df = df.withColumn(param, func(df[param]))
        return df

    @add_attr(cols)
    def rename(columns_old_new=None, func=None):
        """"
        This functions change the name of a column(s) dataFrame.
        :param columns_old_new: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
        :param func: can be lower, upper or any string transformation function
        """

        df = self
        # Apply a transformation function to the param string
        if isfunction(func):
            exprs = [
                F.col(c).alias(func(c)) for c in df.columns
            ]
            df = df.select(*exprs)
        else:
            # Check that the 1st element in the tuple is a valid set of columns
            assert validate_columns_names(self, columns_old_new, 0)
            for c in columns_old_new:
                df = df.withColumnRenamed(c[0], c[1])

        return df

    @add_attr(cols)
    def cast(cols_and_types):
        """
        Cast a column to a var type
        :param self:
        :param cols_and_types:
                List of tuples of column names and types to be casted. This variable should have the
                following structure:

                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]

                The first parameter in each tuple is the column name, the second is the final datatype of column after
                the transformation is made.
        :return:
        """

        assert validate_columns_names(self, cols_and_types, 0)

        def _cast(attr):
            return F.col(attr[0]) \
                .cast(DICT_TYPES[TYPES[attr[1]]]) \
                .alias(attr[0])

        df = apply(cols_and_types, _cast, True)

        return df

    @add_attr(cols)
    def move(column, ref_col, position):
        """
        Move a column to specific position
        :param column:
        :param ref_col:
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
    def keep(columns, regex=None):
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
    def sort(reverse=False):
        columns = sorted(self.columns, reverse=reverse)
        return self.select(columns)
        pass

    @add_attr(cols)
    def drop(columns=None, regex=None):
        df = self
        if regex:
            r = re.compile(regex)
            columns = list(filter(r.match, self.columns))

        columns = parse_columns(self, columns)

        for column in columns:
            df = df.drop(column)
        return df

    @add_attr(cols)
    # Quantile statistics
    @add_attr(cols)
    def _agg(agg, columns):
        """
        Helper function to manage aggregation functions
        :param agg: Aggregation function from spark
        :param columns: list of columns names or a string (a column name).
        :return:
        """
        columns = parse_columns(self, columns)

        # Aggregate
        result = list(map(lambda c: self.agg({c: agg}).collect()[0][0], columns))

        column_result = dict(zip(columns, result))

        # if the list has one elment return just a single element
        return column_result

    @add_attr(cols)
    def min(columns):
        """
        Return the min value from a column dataframe
        :param columns: '*', list of columns names or a string (a column name).
        :return:
        """
        return _agg("min", columns)

    @add_attr(cols)
    def max(columns):
        """
        Return the max value from a column dataframe
        :param columns: '*', list of columns names or a string (a column name).
        :return:
        """
        return _agg("max", columns)

    @add_attr(cols)
    def range(columns):
        """
        Return the range form the min to the max value
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)

        range = []
        for c in columns:
            max_val = max(c)[c]
            min_val = min(c)[c]
            range.append({c: {'min': min_val, 'max': max_val}})

        return range

    @add_attr(cols)
    def median(columns):
        """
        Return the median of a column dataframe
        :param columns:
        :return:
        """

        result = list(map(lambda c: self.approxQuantile(columns, [0.5], 0)[0][0], columns))

        result = val_to_list(result)
        columns = val_to_list(columns)

        return dict(zip(columns, result))

    # Descriptive Analytics
    @add_attr(cols)
    def stddev(columns):
        """
        Return the standard deviation of a column dataframe
        :param columns:
        :return:
        """
        return _agg("stddev", columns)

    @add_attr(cols)
    def kurt(columns):
        """
        Return the kurtosis of a column dataframe
        :param columns:
        :return:
        """
        return _agg("kurtosis", columns)

    @add_attr(cols)
    def mean(columns):
        """
        Return the mean of a column dataframe
        :param columns:
        :return:
        """
        return _agg("mean", columns)

    @add_attr(cols)
    def skewness(columns):
        """
        Return the skewness of a column dataframe
        :param columns:
        :return:
        """
        return _agg("skewness", columns)

    @add_attr(cols)
    def sum(columns):
        """
        Return the sum of a column dataframe
        :param columns:
        :return:
        """
        return _agg("skewness", columns)

    @add_attr(cols)
    def variance(columns):
        """
        Return the variance of a column dataframe
        :param columns:
        :return:
        """
        return _agg("variance", columns)

    # String Operations
    @add_attr(cols)
    def lower(columns):
        """
        Lowercase all the string in a column
        :param columns:
        :return:
        """
        return apply(columns, F.lower)

    @add_attr(cols)
    def upper(columns):
        """
        Uppercase all the strings column
        :param columns:
        :return:
        """
        return apply(columns, F.upper)

    @add_attr(cols)
    def trim(columns):
        """
        Trim the string in a column
        :param columns:
        :return:
        """
        return apply(columns, F.trim)

    @add_attr(cols)
    def reverse(columns):
        """
        Reverse the order of all the string in a column
        :param columns:
        :return:
        """
        return apply(columns, F.reverse)

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

    @add_attr(cols)
    def remove_accents(columns):
        """
        Remove accents in specific columns
        :param columns:
        :return:
        """
        return apply(columns, _remove_accents)

    @add_attr(cols)
    def split(column, mark, get=None, n=None):
        df = self
        split_col = F.split(df[column], mark)

        # assert isinstance(get, int), "Error: get must be an integer"
        if get:
            df = df.withColumn('COL_' + str(get), split_col.getItem(get))
        if n:
            for p in builtins.range(n):
                df = df.withColumn('COL_' + str(p), split_col.getItem(p))

        return df

    return cols
