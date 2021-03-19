import functools
import operator
from functools import reduce

from multipledispatch import dispatch
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Helpers
from optimus.engines.spark.audf import filter_row_by_data_type as fbdt
from optimus.engines.base.dask.rows import DaskBaseRows
from optimus.engines.spark.create import Create
from optimus.helpers.check import is_spark_dataframe
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.functions_spark import append as append_df
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_tuples, is_list_of_str_or_int
from optimus.infer_spark import is_list_of_spark_dataframes

from optimus.engines.base.meta import Meta

class Rows(DaskBaseRows):

    def __init__(self, df):
        super(DaskBaseRows, self).__init__(df)

    @staticmethod
    def create_id(column="id") -> DataFrame:
        """
        Create a unique id for every row.
        :param column: Columns to be processed
        :return:
        """

        return self.withColumn(column, F.monotonically_increasing_id())

    @staticmethod
    def append(rows) -> DataFrame:
        """
        Append a row at the end of a dataframe
        :param rows: List of tuples or dataframes to be appended
        :return: Spark DataFrame
        """
        df = self

        if is_list_of_tuples(rows):
            columns = [str(i) for i in range(df.cols.count())]
            if not is_list_of_tuples(rows):
                rows = [tuple(rows)]
            new_row = Create().df(columns, rows)
            df_result = df.union(new_row)

        elif is_list_of_spark_dataframes(rows) or is_spark_dataframe(rows):
            row = val_to_list(rows)
            row.insert(0, df)
            df_result = append_df(row, like="rows")
        else:
            RaiseIt.type_error(rows, ["list of tuples", "list of dataframes"])

        df_result = df_result.meta.preserve(None, Actions.APPEND.value, df.cols.names())

        return df_result

    def select(self, condition):
        """
        Alias of Spark filter function. Return rows that match a expression
        :param condition:
        :return: Spark DataFrame
        """
        dfd = self.root.data
        dfd = dfd.filter(condition)
        df = self.root.new(dfd)
        df.meta = Meta.action(df.meta, None, Actions.SORT_ROW.value, df.cols.names())

        return odf

    def count(self, compute=True) -> int:
        """
        Count dataframe rows
        """

        return self.root.data.count()

    @staticmethod
    def select(*args, **kwargs) -> DataFrame:
        """
        Alias of Spark filter function. Return rows that match a expression
        :param args:
        :param kwargs:
        :return: Spark DataFrame
        """
        return self.root.data.filter(*args, **kwargs)

    def _sort_multiple(self, dfd, col_sort, meta):
        func = []
        for cs in col_sort:
            col_name = one_list_to_val(cs[0])
            order = cs[1]

            if order == "asc":
                sort_func = F.asc
            elif order == "desc":
                sort_func = F.desc
            else:
                RaiseIt.value_error(sort_func, ["asc", "desc"])

            func.append(sort_func(col_name))
            meta = Meta.action(meta, Actions.SORT_ROW.value, col_name)

        dfd = dfd.sort(*func)
        return dfd, meta


    def _sort(self):
        pass


    def drop(self,where=None) -> DataFrame:
        """
        Drop a row depending on a dataframe expression
        :param where: Expression used to drop the row
        :return: Spark DataFrame
        """
        df = self.paren.data
        df = df.where(~where)
        df.meta = Meta.action(df.meta, None, Actions.DROP_ROW.value, df.cols.names())
        return df

    @staticmethod
    def between(columns, lower_bound=None, upper_bound=None, invert=False, equal=False, bounds=None) -> DataFrame:
        """
                Trim values at input thresholds
                :param upper_bound:
                :param lower_bound:
                :param columns: Columns to be trimmed
                :param bounds:
                :param invert:
                :param equal:
                :return:
                """
        # TODO: should process string or dates
        columns = parse_columns(self.root, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
        if bounds is None:
            bounds = [(lower_bound, upper_bound)]

        def _between(_col_name):

            if invert is False and equal is False:
                op1 = operator.gt
                op2 = operator.lt
                opb = operator.__and__

            elif invert is False and equal is True:
                op1 = operator.ge
                op2 = operator.le
                opb = operator.__and__

            elif invert is True and equal is False:
                op1 = operator.lt
                op2 = operator.gt
                opb = operator.__or__

            elif invert is True and equal is True:
                op1 = operator.le
                op2 = operator.ge
                opb = operator.__or__

            sub_query = []
            for bound in bounds:
                _lower_bound, _upper_bound = bound
                sub_query.append(opb(op1(F.col(_col_name), _lower_bound), op2(F.col(_col_name), _upper_bound)))
            query = functools.reduce(operator.__or__, sub_query)

            return query

        df = self
        for col_name in columns:
            df = df.rows.select(_between(col_name))
        df.meta = Meta.action(df.meta, None, Actions.DROP_ROW.value, df.cols.names())
        return df

    @staticmethod
    def drop_by_dtypes(input_cols, data_type=None):
        """
        Drop rows by cell data type
        :param input_cols: Column in which the filter is going to be applied
        :param data_type: filter by string, integer, float or boolean
        :return: Spark DataFrame
        """
        df = self
        input_cols = parse_columns(df, input_cols)
        df = df.rows.drop(fbdt(input_cols, data_type))
        df.meta = Meta.action(df.meta, None, Actions.DROP_ROW.value, df.cols.names())
        return df

    @staticmethod
    def drop_na(input_cols, how="any") -> DataFrame:
        """
        Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
        'any' of the values is null.

        :param input_cols:
        :param how: ‘any’ or ‘all’. If ‘any’, drop a row if it contains any nulls. If ‘all’, drop a row only if all its
        values are null. The default is 'all'.
        :return: Returns a new DataFrame omitting rows with null values.
        """
        df = self
        input_cols = parse_columns(self.root, input_cols)

        df = df.dropna(how, subset=input_cols)
        df.meta = Meta.action(df.meta, None, Actions.DROP_ROW.value, df.cols.names())
        return df

    @staticmethod
    def drop_duplicates(input_cols=None) -> DataFrame:
        """
        Drop duplicates values in a dataframe
        :param input_cols: List of columns to make the comparison, this only  will consider this subset of columns,
        :return: Return a new DataFrame with duplicate rows removed
        """
        # TODO:
        #  add param
        #  first : Drop duplicates except for the first occurrence.
        #  last : Drop duplicates except for the last occurrence.
        #  all: Drop all duplicates except for the last occurrence.

        df = self
        input_cols = parse_columns(self.root, input_cols)
        df = df.drop_duplicates(subset=input_cols)
        df.meta = Meta.action(df.meta, None, Actions.DROP_ROW.value, df.cols.names())
        return df

    @staticmethod
    def drop_first() -> DataFrame:
        """
        Remove first row in a dataframe
        :return: Spark DataFrame
        """
        df = self
        df = df.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])
        df.meta = Meta.action(df.meta, None, Actions.DROP_ROW.value, df.cols.names())
        return df


    def limit(self, count):
        """
        Limit the number of rows
        :param count:
        :return:
        """
        dfd = self.root.data
        return self.root.new(dfd.limit(count))

    # TODO: Merge with select
    @staticmethod
    def is_in(input_cols, values) -> DataFrame:
        """
        Filter rows which columns match a specific value
        :return: Spark DataFrame
        """
        df = self

        # Ensure that we have a list
        values = val_to_list(values)

        # Create column/value expression
        column_expr = [(F.col(input_cols) == v) for v in values]

        # Concat expression with and logical or
        expr = reduce(lambda a, b: a | b, column_expr)
        df = df.rows.select(expr)
        df.meta = Meta.action(df.meta, None, Actions.DROP_ROW.value, input_cols)
        return df

    @staticmethod
    def unnest(input_cols) -> DataFrame:
        """
        Convert a list in a rows to multiple rows with the list values
        :param input_cols:
        :return:
        """
        input_cols = parse_columns(self.root, input_cols)[0]
        df = self.root
        dfd = df.data
        dfd = dfd.withColumn(input_cols, F.explode(input_cols))
        meta = Meta.set(df.meta, value=df.meta.preserve(None, Actions.DROP_ROW.value, input_cols).get())
        return df.new(dfd, meta=meta)

    def approx_count(self, timeout=1000, confidence=0.90) -> DataFrame:
        """
        Return aprox rows count
        :param timeout:
        :param confidence:
        :return:
        """
        dfd = self.root.data
        return dfd.rdd.countApprox(timeout, confidence)
