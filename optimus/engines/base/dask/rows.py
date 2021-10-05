import functools
import operator
from optimus.engines.base.distributed.rows import DistributedBaseRows

from optimus.engines.base.meta import Meta
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions


class DaskBaseRows(DistributedBaseRows):

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

        return self.root.new(
            self.root._base_to_dfd(
                df.cols.select("*").data.head(count, npartitions=-1), df.partitions() if count > 100 else 1
            )
        )

    def _drop_duplicated_builtin(self, cols):

        df = self.root
        dfd = df.data

        dfd = dfd.drop_duplicates(subset=cols).reset_index(drop=True)

        meta = self.root.meta
        meta = Meta.action(meta, Actions.DROP_ROW.value, df.cols.names())

        return self.root.new(dfd, meta=meta)
                
    def drop_duplicated(self, cols="*", keep="first", how="any"):
        # not supported arguments on dask
        if how == "all" and keep == "first":
            df = self.root
            cols = parse_columns(df, cols)
            return self._drop_duplicated_builtin(cols)
        return self._mask(cols, func=self.root.mask.duplicated, drop=True, keep=keep, how=how)

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

    def between_index(self, lower_bound=None, upper_bound=None, cols="*"):
        """

        :param columns:
        :param lower_bound:
        :param upper_bound:
        :return:
        """
        df = self.root
        dfd = df.data

        if lower_bound is not None:
            length = len(dfd)
            dfd = dfd.tail(length - lower_bound, compute=False)

            if upper_bound is not None:
                upper_bound -= lower_bound

        if upper_bound is not None:
            dfd = dfd.head(upper_bound, compute=False)
        
        if lower_bound is not None or upper_bound is not None:
            dfd = dfd.reset_index(drop=True)

        cols = parse_columns(df, cols)

        return self.root.new(dfd[cols])
