from multipledispatch import dispatch
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Helpers
import optimus.create as op
from optimus.functions import filter_row_by_data_type as fbdt
from optimus.helpers.checkit import is_list_of_str_or_int
from optimus.helpers.constants import *
from optimus.helpers.decorators import *
from optimus.helpers.functions import validate_columns_names, parse_columns, one_list_to_val


def rows(self):
    @add_attr(rows)
    def append(row):
        """
        Append a row at the end of a dataframe
        :param row: List of values to be appended
        :return: Spark DataFrame
        """

        df = self

        assert isinstance(row, list), "Error: row must me a list"
        assert len(row) > 0, "Error: row list must be greater that 0"
        assert len(df.dtypes) == len(row), "Error row must be the same length of the dataframe"

        cols = []
        values = []
        for d, r in zip(df.dtypes, row):
            col_name = d[0]
            data_type = d[1]
            if data_type in SPARK_DTYPES_DICT_OBJECTS:
                cols.append((col_name, (SPARK_DTYPES_DICT_OBJECTS[data_type]), True))
                values.append(r)

        values = [tuple(values)]
        new_row = op.Create.data_frame(cols, values)

        return df.union(new_row)

    @add_attr(rows)
    def select_by_dtypes(col_name, data_type=None):
        """
        This function has built in order to filter some type of row depending of the var type detected by python
        for Example if you have a column with
        | a |
        | 1 |
        | b |

        and you filter by type = integer you will get

        | 1 |

        :param col_name: Column to be filtered
        :param data_type: Datatype use filter values
        :return: Spark DataFrame
        """
        col_name = parse_columns(self, col_name)

        return self.where(fbdt(col_name, data_type))

    @add_attr(rows)
    def select(*args, **kwargs):
        """
        Alias of Spark filter function. Return rows that match a expression
        :param args:
        :param kwargs:
        :return: Spark DataFrame
        """
        return self.filter(*args, **kwargs)

    @add_attr(rows)
    @dispatch(str)
    def sort(columns):
        """
        Sort column by row
        """
        columns = parse_columns(self, columns)
        return self.rows.sort([(columns, "desc",)])

    @add_attr(rows)
    @dispatch(str, str)
    def sort(columns, order="desc"):
        """
        Sort column by row
        """
        columns = parse_columns(self, columns)
        return self.rows.sort([(columns, order,)])

    @add_attr(rows)
    @dispatch(list)
    def sort(col_sort):
        """
        Sort columns taking in account multiple columns
        :param col_sort: column and sort type combination (col_name, "asc")
        :type col_sort: list of tuples
        """
        # If a list of columns names are given order this by desc. If you need to specify the order of every
        # column use a list of tuples (col_name, "asc")
        t = []
        if is_list_of_str_or_int(col_sort):
            for col_name in col_sort:
                t.append(tuple([col_name, "desc"]))
            col_sort = t

        func = []

        for cs in col_sort:
            col_name = one_list_to_val(cs[0])
            order = cs[1]


            if order == "asc":
                sort_func = F.asc
            elif order == "desc":
                sort_func = F.desc
            func.append(sort_func(col_name))
        df = self.sort(*func)
        return df

    @add_attr(rows)
    def drop(where=None):
        """
        Drop a row depending on a dataframe expression
        :param where: Expression used to drop the row
        :return: Spark DataFrame
        """
        return self.where(~where)

    @add_attr(rows)
    def drop_by_dtypes(col_name, data_type=None):
        """
        Drop rows by cell data type
        :param col_name: Column in which the filter is going to be apllied
        :param data_type: filter by string, integer, float or boolean
        :return: Spark DataFrame
        """
        validate_columns_names(self, col_name)
        return self.rows.drop(fbdt(col_name, data_type))

    @add_attr(rows)
    def drop_na(columns, how="all"):
        """
        Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
        'any' of the values is null.

        :param columns:
        :param how: ‘any’ or ‘all’. If ‘any’, drop a row if it contains any nulls. If ‘all’, drop a row only if all its
        values are null. The default is 'all'.
        :return: Returns a new DataFrame omitting rows with null values.
        """

        columns = parse_columns(self, columns)

        return self.dropna(how, subset=columns)

    @add_attr(rows)
    def drop_duplicates(columns=None):
        """
        Drop duplicates values in a dataframe
        :param columns: List of columns to make the comparison, this only  will consider this subset of columns,
        for dropping duplicates. The default behavior will only drop the identical rows.
        :return: Return a new DataFrame with duplicate rows removed
        """

        columns = parse_columns(self, columns)
        return self.drop_duplicates(subset=columns)

    @add_attr(rows)
    def drop_first():
        """
        Remove first row in a dataframe
        :return: Spark DataFrame
        """
        return self.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

    return rows


DataFrame.rows = property(rows)
