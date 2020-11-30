import functools
import operator

import dask.array as da
import dask.dataframe as  dd
import pandas as pd
from multipledispatch import dispatch

from optimus.engines.base.rows import BaseRows
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str_or_int, is_list


class DaskBaseRows(BaseRows):
    """Base class for all Rows implementations"""

    def __init__(self, parent):
        # self.parent = parent
        super().__init__(parent)
        # super(DaskBaseRows, self).__init__(parent)

    def create_id(self, column="id"):
        # Reference https://github.com/dask/dask/issues/1426
        df = self.root.data
        # print(df)
        a = da.arange(df.divisions[-1] + 1, chunks=df.divisions[1:])
        df[column] = dd.from_dask_array(a)
        return df

    def append(self, odfs, cols_map):
        """
        Appends 2 or more dataframes
        :param dfs:
        :param cols_map:
        """
        every_odf = [self.root, *odfs]
        rename = [[] for _ in self.root]
        for key in cols_map:
            assert len(cols_map[key]) == len(every_odf)
            for i in range(len(cols_map[key])):
                col_name = cols_map[key][i]
                if col_name:
                    rename[i] = [*rename[i], (col_name, "__output_column__" + key)]
        for i in range(len(rename)):
            every_odf[i] = every_odf[i].cols.rename(rename[i])
        df = every_odf[0].data
        for i in range(len(every_odf)):
            if i != 0:
                df = df.append(every_odf[i].data)
        odf = self.root.new(df)
        odf = odf.cols.rename([("__output_column__" + key, key) for key in cols_map])
        odf = odf.cols.select([*cols_map.keys()])
        return odf.reset_index(drop=True)

    # def append(self, rows):
    #     """
    #
    #     :param rows:
    #     :return:
    #     """
    #     df = self.root.data
    #
    #     if is_list(rows):
    #         rows = dd.from_pandas(pd.DataFrame(rows), npartitions=1)
    #
    #     # Can not concatenate dataframe with not string columns names
    #     rows.columns = df.columns
    #
    #     df = dd.concat([df, rows], axis=0, interleave_partitions=True)
    #
    #     return df

    def limit(self, count):
        """
        Limit the number of rows
        :param count:
        :return:
        """
        df = self.root.data
        # Reference https://stackoverflow.com/questions/49139371/slicing-out-a-few-rows-from-a-dask-dataframe

        if count is None:
            return df

        length_df = len(df)

        if length_df == 0:
            limit = 0
        else:
            limit = count / length_df

            # Param frac can not be greater than 1
            limit = 1 if limit > 1 else limit

        return self.root.new(df.sample(frac=limit))
        # # TODO. This is totally unreliable to use with big data because is going to bring all the data to the client.
        # return self.parent.new(pandas_to_dask_dataframe(df.head(count)))

    def count(self, compute=True) -> int:
        """
        Count dataframe rows
        """
        df = self.root.data
        # TODO: Be sure that we need the compute param
        if compute is True:
            result = len(df.index)
        else:
            result = len(df.index)
        return result

    @dispatch(str, str)
    def sort(self, input_cols):
        df = self.root.data
        input_cols = parse_columns(df, input_cols)
        return df.rows.sort([(input_cols, "desc",)])

    @dispatch(str, str)
    def sort(self, columns, order="desc"):
        """
        Sort column by row
        """
        df = self.root.data
        columns = parse_columns(df, columns)
        return df.rows.sort([(columns, order,)])

    @dispatch(list)
    def sort(self, col_sort):
        """
        Sort rows taking into account multiple columns
        :param col_sort: column and sort type combination (col_name, "asc")
        :type col_sort: list of tuples
        """
        # If a list of columns names are given order this by desc. If you need to specify the order of every
        # column use a list of tuples (col_name, "asc")
        odf = self.root
        meta = odf.meta

        t = []
        if is_list_of_str_or_int(col_sort):
            for col_name in col_sort:
                t.append(tuple([col_name, "desc"]))
            col_sort = t

        for cs in col_sort:
            # print(col_sort)
            col_name = one_list_to_val(cs[0])
            order = cs[1]

            if order != "asc" and order != "desc":
                RaiseIt.value_error(order, ["asc", "desc"])

            def func(pdf):
                return pdf.sort_values(col_name, ascending=True if order == "asc" else False)

            odf = odf.map_partitions(func)

            meta = meta.action(Actions.SORT_ROW.value, col_name)

            # c = odf.cols.names()
            # It seems that is on possible to order rows in Dask using set_index. It only return data in asc way.
            # We should fins a way to make it work desc and form multiple columns
            # odf = odf.set_index(col_name).reset_index()[c]

        return odf.new(odf.data, meta=meta)

    def between_index(self, columns, lower_bound=None, upper_bound=None):
        """

        :param columns:
        :param lower_bound:
        :param upper_bound:
        :return:
        """
        df = self.root.data
        columns = parse_columns(df, columns)
        return df[lower_bound: upper_bound][columns]

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
        odf = self.root
        # TODO: should process string or dates
        # columns = parse_columns(odf, columns, filter_by_column_dtypes=odf.constants.NUMERIC_TYPES)
        columns = parse_columns(odf, columns)
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
                sub_query.append(opb(op1(odf[_col_name], _lower_bound), op2(odf[_col_name], _upper_bound)))
            query = functools.reduce(operator.__or__, sub_query)

            return query

        for col_name in columns:
            odf = odf.rows.select(_between(col_name))
        meta = odf.meta.action(Actions.DROP_ROW.value, odf.cols.names())
        return odf.new(odf.data, meta=meta)

    def drop_by_dtypes(self, input_cols, data_type=None):
        df = self.root.data
        return df

    def drop_duplicates(self, keep="first", subset=None):
        """
        Drop duplicates values in a dataframe
        :param subset: List of columns to make the comparison, this only  will consider this subset of columns,
        :return: Return a new DataFrame with duplicate rows removed
        :return:
        """
        df = self.root.data
        subset = parse_columns(df, subset)
        subset = val_to_list(subset)
        df = df.drop_duplicates(keep=keep, subset=subset)

        return self.root.new(df)

    def is_in(self, input_cols, values, output_cols=None):

        # return self.apply()
        def _is_in(value, *args):
            _values = args
            return value.isin(_values)

        df = self.root
        return df.cols.apply(input_cols, func=_is_in, args=(values,), output_cols=output_cols)

        # df = self.parent.data
        # columns = prepare_columns(self.parent, input_cols, output_cols, accepts_missing_cols=True)
        # kw_columns ={}
        # for input_col, output_col in columns:
        #     kw_columns[output_col]= df[input_col].isin(values)
        #
        # df = df.assign(**kw_columns)
        # return self.parent.new(df)

    def unnest(self, input_cols):
        df = self.root.data
        return df

    def approx_count(self):
        """
        Aprox rows count
        :return:
        """
        df = self.root
        return df.rows.count()
