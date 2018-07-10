from optimus.helpers.decorators import add_method
from pyspark.sql import DataFrame

from pyspark.sql.column import Column
# from pyspark.sql.functions import lit
from pyspark.sql.functions import *
import re

# Library used for method overloading using decorators
from multipledispatch import dispatch

# Helpers
from optimus.helpers.validators import *
from optimus.helpers.constants import *


class Columns:
    def __init__(self, df):

        self._df = df

    @dispatch(str, object)
    def create(self, name=None, value=None):
        """

        :param name:
        :param value:
        :return:
        """

        assert isinstance(name, str), "name param must be a string"
        assert isinstance(value, (int, float, str, Column)), "object param must be a number, str or a Column "

        if isinstance(value, (int, str, float)):
            value = lit(value)

        return self._df.withColumn(name, value)

    @dispatch(list)
    def create(self, columns=None):
        """

        :param columns:
        :return:
        """

        for c in columns:
            self._df = self.create(
                c[0],
                c[1]
            )
        return self._df

    def select(self, columns=None, regex=None):
        """

        :param columns:
        :param regex:
        :return:
        """

        if regex:
            r = re.compile(regex)
            columns = list(filter(r.match, self._df.columns))

        def get_column(column):
            if isinstance(column, int):
                col_name = self._df.columns[column]
            elif isinstance(column, str):
                col_name = column
            return col_name

        return self._df.select(list(map(get_column, columns)))

    def rename(self, columns_pair):
        """"
        This functions change the name of a column(s) dataFrame.
        :param columns_pair: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
        """
        # Check that the 1st element in the tuple is a valis set of columns
        assert validate_columns_names(self._df, columns_pair, 0)

        # Rename cols
        def _rename(column, t):
            return col(column[0]).alias(column[1])

        columns = self._recreate_columns(columns_pair, _rename)
        #print(columns)
        return self._df.select(columns)

    def move(self, from_column, to_column):
        print("Column Moved. int params")

    def cast(self, cols_and_types):
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

        # Check if columnNames to be process are in the dataframe
        assert validate_columns_names(cols_and_types, 0)

        def _cast(column, t):
            return col(column).cast(DICT_TYPES[TYPES[t[1]]]).alias(column)

        columns = self._recreate_columns(cols_and_types, _cast)

        return self[columns]

    def drop(self, by_index=None, by_name=None, regex=None):
        print("Drop column")

    def _recreate_columns(self, col_pair, func):
        """
        Operation as rename or cast need to reconstruct the columns with new types or names.
        This is a helper function handle it
        :param col_pair:
        :param func:
        :return:
        """
        print(self._df.columns)
        columns = []
        for c in self._df.columns:
            for t in col_pair:
                # if the column is in cols_and_types
                if t[0] == c:
                    new_col = func(c, t)
                else:
                    # Else, Add the column without modification
                    new_col = col(c)
            columns.append(new_col)
        return columns

@add_method(DataFrame)
def cols(self):
    if not hasattr(self, 'columns_'):
        self.columns_ = Columns(self)
        #print(self.columns)
    return self.columns_
