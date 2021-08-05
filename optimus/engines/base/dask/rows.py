import functools
import operator

import dask.array as da
import dask.dataframe as dd
from dask.delayed import delayed

from optimus.engines.base.meta import Meta
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions


class DaskBaseRows():

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
        # columns = parse_columns(df, columns, filter_by_column_types=df.constants.NUMERIC_TYPES)
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

    def approx_count(self):
        """
        Aprox rows count
        :return:
        """
        df = self.root
        return df.rows.count()
