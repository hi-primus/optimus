import numpy as np
from dask.dataframe.core import DataFrame

from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.columns import parse_columns, get_output_cols, check_column_numbers
from optimus.helpers.converter import format_dict


# This implementation works for Dask

def cols(self: DataFrame):
    class Cols(DaskBaseColumns):
        def __init__(self, df):
            super(DaskBaseColumns, self).__init__(df)

        @staticmethod
        def abs(input_cols, output_cols=None):
            """
            Apply abs to the values in a column
            :param input_cols:
            :param output_cols:
            :return:
            """
            df = self
            input_cols = parse_columns(df, input_cols, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
            output_cols = get_output_cols(input_cols, output_cols)

            check_column_numbers(output_cols, "*")
            # Abs not accepts column's string names. Convert to Spark Column

            # TODO: make this in one pass.

            for input_col, output_col in zip(input_cols, output_cols):
                df[output_col] = df.compute(np.abs(df[input_col]))
            return df

        @staticmethod
        def mode(columns):
            """
            Return the column mode
            :param columns: '*', list of columns names or a single column name.
            :return:
            """

            # Reference https://stackoverflow.com/questions/46080171/constructing-mode-and-corresponding-count-functions-using-custom-aggregation-fun
            def chunk(s):
                # for the comments, assume only a single grouping column, the
                # implementation can handle multiple group columns.
                #
                # s is a grouped series. value_counts creates a multi-series like
                # (group, value): count
                return s.value_counts()

            def agg(s):
                # s is a grouped multi-index series. In .apply the full sub-df will passed
                # multi-index and all. Group on the value level and sum the counts. The
                # result of the lambda function is a series. Therefore, the result of the
                # apply is a multi-index series like (group, value): count
                # return s.apply(lambda s: s.groupby(level=-1).sum())

                # faster version using pandas internals
                s = s._selected_obj
                return s.groupby(level=list(range(s.index.nlevels))).sum()

            def finalize(s):
                # s is a multi-index series of the form (group, value): count. First
                # manually group on the group part of the index. The lambda will receive a
                # sub-series with multi index. Next, drop the group part from the index.
                # Finally, determine the index with the maximum value, i.e., the mode.
                level = list(range(s.index.nlevels - 1))
                return (
                    s.groupby(level=level)
                        .apply(lambda s: s.reset_index(level=level, drop=True).idxmax())
                )

            mode = dd.Aggregation('mode', chunk, agg, finalize)

            df = self

            mode_result = df.groupby(['price']).agg({'price': mode}).compute()
            return format_dict(mode_result)

    return Cols(self)


DataFrame.cols = property(cols)
