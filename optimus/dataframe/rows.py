from functools import reduce

from multipledispatch import dispatch
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Helpers
import optimus as op
from optimus import val_to_list
from optimus.audf import filter_row_by_data_type as fbdt
from optimus.helpers.check import is_list_of_str_or_int, is_list_of_tuples, is_list_of_dataframes, is_dataframe
from optimus.helpers.columns import parse_columns, validate_columns_names
from optimus.helpers.converter import one_list_to_val
from optimus.helpers.decorators import *
from optimus.helpers.functions import append as append_df
from optimus.helpers.raiseit import RaiseIt


def rows(self):
    @add_attr(rows)
    def append(rows):
        """
        Append a row at the end of a dataframe
        :param rows: List of values or tuples to be appended
        :return: Spark DataFrame
        """
        df = self

        if is_list_of_tuples(rows):
            columns = [str(i) for i in range(df.cols.count())]
            if not is_list_of_tuples(rows):
                rows = [tuple(rows)]
            new_row = op.Create.df(columns, rows)
            df_result = df.union(new_row)

        elif is_list_of_dataframes(rows) or is_dataframe(rows):
            row = val_to_list(rows)
            row.insert(0, df)
            df_result = append_df(row, like="rows")
        else:
            RaiseIt.type_error(rows, ["list of tuples", "list of dataframes"])
        return df_result

    @add_attr(rows)
    def select_by_dtypes(input_cols, data_type=None):
        """
        This function has built in order to filter some type of row depending of the var type detected by python
        for Example if you have a column with
        | a |
        | 1 |
        | b |

        and you filter by type = integer you will get

        | 1 |

        :param input_cols: Column to be filtered
        :param data_type: Datatype use filter values
        :return: Spark DataFrame
        """
        input_cols = parse_columns(self, input_cols)

        return self.where(fbdt(input_cols, data_type))

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
    def to_list(input_cols):
        """
        Output rows as list
        :param input_cols:
        :return:
        """
        input_cols = parse_columns(self, input_cols)
        return self.select(input_cols).rdd.map(lambda x: [v for v in x.asDict().values()]).collect()

    @add_attr(rows)
    @dispatch(str)
    def sort(input_cols):
        """
        Sort column by row
        """
        input_cols = parse_columns(self, input_cols)
        return self.rows.sort([(input_cols, "desc",)])

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
        Sort rows taking in account multiple columns
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
    def drop_by_dtypes(input_cols, data_type=None):
        """
        Drop rows by cell data type
        :param input_cols: Column in which the filter is going to be apllied
        :param data_type: filter by string, integer, float or boolean
        :return: Spark DataFrame
        """
        validate_columns_names(self, input_cols)
        return self.rows.drop(fbdt(input_cols, data_type))

    @add_attr(rows)
    def drop_na(input_cols, how="any"):
        """
        Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
        'any' of the values is null.

        :param input_cols:
        :param how: ‘any’ or ‘all’. If ‘any’, drop a row if it contains any nulls. If ‘all’, drop a row only if all its
        values are null. The default is 'all'.
        :return: Returns a new DataFrame omitting rows with null values.
        """

        input_cols = parse_columns(self, input_cols)

        return self.dropna(how, subset=input_cols)

    @add_attr(rows)
    def drop_duplicates(input_cols=None, keep="first"):
        """
        Drop duplicates values in a dataframe
        :param input_cols: List of columns to make the comparison, this only  will consider this subset of columns,
        :param keep: keep or delete the duplicated row
        for dropping duplicates. The default behavior will only drop the whole identical rows.
        :return: Return a new DataFrame with duplicate rows removed
        """
        # TODO:
        #  add param
        #  first : Drop duplicates except for the first occurrence.
        #  last : Drop duplicates except for the last occurrence.
        #  all: Drop all duplicates except for the last occurrence.

        input_cols = parse_columns(self, input_cols)
        return self.drop_duplicates(subset=input_cols)

    @add_attr(rows)
    def drop_first():
        """
        Remove first row in a dataframe
        :return: Spark DataFrame
        """
        return self.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])

    # TODO: Merge with select
    @add_attr(rows)
    def is_in(columns, values):
        """
        Filter rows which columns that match a specific value
        :return: Spark DataFrame
        """

        # Ensure that we have a list
        values = val_to_list(values)

        # Create column/value expression
        column_expr = [(F.col(columns) == v) for v in values]

        # Concat expression with and logical or
        expr = reduce(lambda a, b: a | b, column_expr)

        return self.rows.select(expr)

    @add_attr(rows)
    def unnest(input_cols):
        """
        Convert a list in a rows to multiple rows with the list values
        :param input_cols:
        :return:
        """
        input_cols = parse_columns(self, input_cols)[0]
        df = self
        return df.withColumn(input_cols, F.explode(input_cols))

    @add_attr(rows)
    def approx_count(timeout=1000, confidence=0.90):
        """
        Return aprox rows count
        :param timeout:
        :param confidence:
        :return:
        """
        return self.rdd.countApprox(timeout, confidence)

    return rows


DataFrame.rows = property(rows)
