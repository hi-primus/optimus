from optimus.helpers.decorators import add_method
from pyspark.sql import DataFrame

from pyspark.sql.functions import *
import re

# Library used for method overloading using decorators
from multipledispatch import dispatch

# Helpers
from optimus.helpers.validators import *
from optimus.helpers.constants import *


@add_method(DataFrame)
def cols(self):
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
            value = lit(value)

        return self.withColumn(name, value)

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
                value = lit(value)
            df = df.withColumn(name, value)
        return df

    def select(columns=None, regex=None):
        """

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

    def rename(columns_pair):
        """"
        This functions change the name of a column(s) dataFrame.
        :param columns_pair: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
        """
        # Check that the 1st element in the tuple is a valis set of columns
        assert validate_columns_names(self, columns_pair, 0)

        # Rename cols
        def _rename(column, t):
            return col(column).alias(t)

        columns = _recreate_columns(columns_pair, _rename)

        return self.select(columns)

    def move(column, ref_col, position):
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

        def _cast(column, t):
            return col(column).cast(DICT_TYPES[TYPES[t]]).alias(column)

        columns = _recreate_columns(cols_and_types, _cast)

        return self[columns]

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

    def drop(columns=None, regex=None):
        df = self
        if regex:
            r = re.compile(regex)
            columns = list(filter(r.match, self.columns))

        columns = parse_columns(self, columns)

        for column in columns:
            df = df.drop(column)
        return df

    def _recreate_columns(col_pair, func):
        """
        Operation as rename or cast need to reconstruct the columns with new types or names.
        This is a helper function handle it
        :param col_pair:
        :param func:
        :return:
        """
        columns = []
        for c in self.columns:
            new_col = ""
            for t in col_pair:
                # if the column is in cols_and_types
                if t[0] == c:
                    new_col = func(c, t[1])
                else:
                    # Else, Add the column without modification
                    new_col = col(c)
            columns.append(new_col)
        return columns

    cols.create = create
    cols.select = select
    cols.rename = rename
    cols.cast = cast
    cols.move = move
    cols.drop = drop
    cols.keep = keep
    cols._recreate_columns = _recreate_columns

    return cols
