from dask.dataframe.core import DataFrame

from optimus.helpers.columns import parse_columns


def rows(self):
    class Rows:
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
        def approx_count():
            """
            Aprox count
            :return:
            """
            return Rows.count()

    return Rows()


DataFrame.rows = property(rows)
