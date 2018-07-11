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


@add_method(DataFrame)
def rows(self):
    @add_attr(rows)
    def create(name=None, value=None):
        pass

    @add_attr(rows)
    def filter(func=None):
        self = self.filter(func)
        pass

    @add_attr(rows)
    def drop_duplicates(columns=None):
        """

        :param columns: List of columns to make the comparison, this only  will consider this subset of columns,
        for dropping duplicates. The default behavior will only drop the identical rows.
        :return: Return a new DataFrame with duplicate rows removed
        """

        columns = self._parse_columns(columns)

        return self.drop_duplicates(columns)

    @add_attr(rows)
    def drop_empty_rows(columns, how="all"):
        """
        Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
        'any' of the values is null.
        :param columns:
        :param how: ‘any’ or ‘all’. If ‘any’, drop a row if it contains any nulls. If ‘all’, drop a row only if all its
        values are null. The default is 'all'.
        :return: Returns a new DataFrame omitting rows with null values.
        """

        assert isinstance(how, str), "Error, how argument provided must be a string."

        assert how == 'all' or (
                how == 'any'), "Error, how only can be 'all' or 'any'."

        columns = self._parse_columns(columns)

        return self._df.dropna(how, columns)

    @add_attr(rows)
    def replace(search, change_to, columns):
        """

        :param self:
        :param search:
        :param change_to:
        :param columns:
        :return:
        """

        columns = self._parse_columns(columns)

        return self.replace(search, change_to, subset=columns)

    @add_attr(rows)
    def drop(func):
        """This function is an alias of filter and where spark functions.
               :param func     func must be an expression with the following form:

                       func = col('col_name') > value.

                       func is an expression where col is a pyspark.sql.function.
               """
        self = self.filter(func)

        # Returning the transformer object for able chaining operations
        return self

    @add_attr(rows)
    def filter(column_name, type):
        """This function has built in order to deleted some type of dataframe """

        validate_columns_names(self, column_name)

        print(column_name)

        # Asserting if dataType argument has a valid type:
        assert (type in ['integer', 'float', 'string',
                         'null']), \
            "Error: dataType only can be one of the followings options: integer, float, string, null."

        # Function for determine if register value is float or int or string:
        def data_type(value):
            try:  # Try to parse (to int) register value
                int(value)
                # Add 1 if suceed:
                return 'integer'
            except ValueError:
                try:
                    # Try to parse (to float) register value
                    float(value)
                    # Add 1 if suceed:
                    return 'float'
                except ValueError:
                    # Then, it is a string
                    return 'string'
            except TypeError:
                return 'null'

        func = F.udf(data_type, StringType())
        df = self

        temp_col_name = "type_optimus"

        df = df.withColumn(
            temp_col_name,
            func(F.col(column_name))).where((F.col(temp_col_name) != type)).drop(temp_col_name)
        return df

    return rows
