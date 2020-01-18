import dask.array as da
import dask.dataframe as  dd
from dask.dataframe.core import DataFrame
from multipledispatch import dispatch

from optimus.audf import filter_row_by_data_type as fbdt
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.converter import one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str_or_int


def rows(self):
    class Rows:
        @staticmethod
        def create_id(column="id") -> DataFrame:
            # Reference https://github.com/dask/dask/issues/1426
            df = self
            print(df)
            a = da.arange(df.divisions[-1] + 1, chunks=df.divisions[1:])
            df[column] = dd.from_dask_array(a)
            return df

        @staticmethod
        def append(rows) -> DataFrame:
            df = self
            return df

        @staticmethod
        def select(condition) -> DataFrame:
            """

            :param condition: a condition like (df.A > 0) & (df.B <= 10)
            :return:
            """
            df = self

            df = df[condition]
            df = df.meta.preserve(self, Actions.SORT_ROW.value, df.cols.names())

            return df

        @staticmethod
        def select_by_dtypes(input_cols, data_type=None) -> DataFrame:
            input_cols = parse_columns(self, input_cols)
            # self.cols.apply()
            # TODO
            return self.select(fbdt(input_cols, data_type))

        @staticmethod
        def count() -> int:
            """
            Count dataframe rows
            """
            return len(self)

        @staticmethod
        def to_list(input_cols):
            """

            :param input_cols:
            :return:
            """
            input_cols = parse_columns(self, input_cols)
            df_list = []
            row_list = []
            for index, row in self[input_cols].iterrows():
                for col_name, value in row.iteritems():
                    row_list.append(value)
                df_list.append(row_list)

            return df_list

        @staticmethod
        @dispatch(str, str)
        def sort(input_cols) -> DataFrame:
            input_cols = parse_columns(self, input_cols)
            return self.rows.sort([(input_cols, "desc",)])

        @staticmethod
        @dispatch(str, str)
        def sort(columns, order="desc") -> DataFrame:
            """
            Sort column by row
            """
            columns = parse_columns(self, columns)
            return self.rows.sort([(columns, order,)])

        @staticmethod
        @dispatch(list)
        def sort(col_sort) -> DataFrame:
            """
            Sort rows taking into account multiple columns
            :param col_sort: column and sort type combination (col_name, "asc")
            :type col_sort: list of tuples
            """
            # If a list of columns names are given order this by desc. If you need to specify the order of every
            # column use a list of tuples (col_name, "asc")
            df = self

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
                else:
                    RaiseIt.value_error(sort_func, ["asc", "desc"])

                func.append(sort_func(col_name))
                df = df.meta.preserve(self, Actions.SORT_ROW.value, col_name)

            df = df.sort(*func)
            return df

        @staticmethod
        def drop(where=None) -> DataFrame:
            df = self
            return df

        @staticmethod
        def between(columns, lower_bound=None, upper_bound=None, invert=False, equal=False,
                    bounds=None) -> DataFrame:
            df = self
            return df

        @staticmethod
        def drop_by_dtypes(input_cols, data_type=None):
            df = self
            return df

        @staticmethod
        def drop_na(input_cols, how="any") -> DataFrame:
            df = self
            return df

        @staticmethod
        def drop_duplicates(input_cols=None) -> DataFrame:
            df = self
            return df

        @staticmethod
        def limit(count) -> DataFrame:
            df = self
            return df

        @staticmethod
        def is_in(input_cols, values) -> DataFrame:
            df = self
            return df

        @staticmethod
        def unnest(input_cols) -> DataFrame:
            df = self
            return df

        @staticmethod
        def approx_count():
            """
            Aprox count
            :return:
            """
            return Rows.count()

    return Rows()


DataFrame.rows = property(rows)
