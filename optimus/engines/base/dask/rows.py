import functools
import operator

import dask.array as da
import dask.dataframe as dd
from dask.delayed import delayed
from multipledispatch import dispatch

from optimus.engines.base.meta import Meta
from optimus.engines.base.rows import BaseRows
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str_or_int, is_list_value


class DaskBaseRows(BaseRows):
    """Base class for all Rows implementations"""

    def __init__(self, parent):
        # self.parent = parent
        super().__init__(parent)
        # super(DaskBaseRows, self).__init__(parent)

    def _reverse(self, dfd):
        @delayed
        def reverse_pdf(pdf):
            return pdf[::-1]

        ds = dfd.to_delayed()
        ds = [reverse_pdf(d) for d in ds][::-1]
        return dd.from_delayed(ds)

    def _sort(self, dfd, col_name, ascending):
        dfd = dfd.set_index(col_name)
        if not ascending:
            dfd = self._reverse(dfd)
        return dfd.reset_index()[self.root.cols.names()]

    def create_id(self, column="id"):
        # Reference https://github.com/dask/dask/issues/1426
        dfd = self.root.data
        # print(dfd)
        a = da.arange(dfd.divisions[-1] + 1, chunks=dfd.divisions[1:])
        dfd[column] = dd.from_dask_array(a)
        return dfd

    def append(self, dfs, names_map=None):
        """
        Appends 2 or more dataframes
        :param dfs:
        :param names_map:
        """
        if not is_list_value(dfs):
            dfs = [dfs]

        every_df = [self.root, *dfs]

        if names_map is not None:
            rename = [[] for _ in every_df]
            for key in names_map:
                assert len(names_map[key]) == len(every_df)
                for i in range(len(names_map[key])):
                    col_name = names_map[key][i]
                    if col_name:
                        rename[i] = [*rename[i], (col_name, "__output_column__" + key)]
            for i in range(len(rename)):
                every_df[i] = every_df[i].cols.rename(rename[i])

        dfd = every_df[0].data
        for i in range(len(every_df)):
            if i != 0:
                dfd = dfd.append(every_df[i].data)
        df = self.root.new(dfd)

        if names_map is not None:
            df = df.cols.rename([("__output_column__" + key, key) for key in names_map])
            df = df.cols.select([*names_map.keys()])

        return df.new(df.data.reset_index(drop=True))

    # def append(self, rows):
    #     """
    #
    #     :param rows:
    #     :return:
    #     """
    #     dfd = self.root.data
    #
    #     if is_list(rows):
    #         rows = dd.from_pandas(pd.DataFrame(rows), npartitions=1)
    #
    #     # Can not concatenate dataframe with not string columns names
    #     rows.columns = dfd.columns
    #
    #     dfd = dd.concat([dfd, rows], axis=0, interleave_partitions=True)
    #
    #     return dfd

    def limit(self, count):
        """
        Limit the number of rows
        :param count:
        :return:
        """
        df = self.root
        # Reference https://stackoverflow.com/questions/49139371/slicing-out-a-few-rows-from-a-dask-dataframe

        if count is None:
            return df

        partitions = df.partitions()

        return self.root.new(self.root._base_to_dfd(df.cols.select("*").data.head(count), partitions))

    def between_index(self, columns, lower_bound=None, upper_bound=None):
        """

        :param columns:
        :param lower_bound:
        :param upper_bound:
        :return:
        """
        dfd = self.root.data
        columns = parse_columns(dfd, columns)
        return self.root.new(dfd[lower_bound: upper_bound][columns])

    def between(self, columns, lower_bound=None, upper_bound=None, invert=False, equal=False,
                bounds=None):
        """
        Trim values at input thresholds
        :param upper_bound:
        :param lower_bound:
        :param columns: Columns to be trimmed
        :param invert:
        :param equal:
        :param bounds:
        :return:
        """
        df = self.root
        # TODO: should process string or dates
        # columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        columns = parse_columns(df, columns)
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
                sub_query.append(opb(op1(df[_col_name], _lower_bound), op2(df[_col_name], _upper_bound)))
            query = functools.reduce(operator.__or__, sub_query)

            return query

        for col_name in columns:
            df = df.rows.select(_between(col_name))
        meta = Meta.action(df.meta, Actions.DROP_ROW.value, df.cols.names())
        return self.root.new(df.data, meta=meta)

    def drop_by_dtypes(self, input_cols, data_type=None):
        df = self.root
        return df

    def drop_duplicates(self, keep="first", subset=None):
        """
        Drop duplicates values in a dataframe
        :param subset: List of columns to make the comparison, this only  will consider this subset of columns,
        :param keep: Row to keep when find a duplicate
        :return: Return a new DataFrame with duplicate rows removed
        :return:
        """
        dfd = self.root.data
        subset = parse_columns(dfd, subset)
        subset = val_to_list(subset)
        dfd = dfd.drop_duplicates(keep=keep, subset=subset)

        return self.root.new(dfd)

        # df = self.parent.data
        # columns = prepare_columns(self.parent, input_cols, output_cols, accepts_missing_cols=True)
        # kw_columns ={}
        # for input_col, output_col in columns:
        #     kw_columns[output_col]= df[input_col].isin(values)
        #
        # df = df.assign(**kw_columns)
        # return self.parent.new(df)

    def unnest(self, input_cols):
        df = self.root
        return df

    def approx_count(self):
        """
        Aprox rows count
        :return:
        """
        df = self.root
        return df.rows.count()
