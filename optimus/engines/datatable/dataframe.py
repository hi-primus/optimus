from optimus.engines.base.dataframe.dataframe import DataFrameBaseDataFrame
from optimus.engines.base.datatable.dataframe import DatatableBaseDataFrame
# from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.engines.pandas.io.save import Save


class DatatableDataFrame(DatatableBaseDataFrame, DataFrameBaseDataFrame):

    def _assign(self, kw_columns: dict):
        df = self.root
        dfd = df.data
        cols_exprs = ({col_name: dfd[col_name] for i, col_name in enumerate(df.cols.names())})
        cols_exprs = {**cols_exprs, **kw_columns}

        return dfd[:, list(cols_exprs.values())]

    def _base_to_dfd(self, pdf, n_partitions):
        pass

    @property
    def rows(self):
        from optimus.engines.datatable.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.datatable.columns import Cols
        return Cols(self)

    @property
    def save(self):
        return Save(self)

    @property
    def functions(self):
        from optimus.engines.datatable.functions import DatatableFunctions
        return DatatableFunctions(self)

    @property
    def mask(self):
        from optimus.engines.datatable.mask import DatatableMask
        return DatatableMask(self)

    @property
    def ml(self):
        from optimus.engines.datatable.ml.models import ML
        return ML(self)

    @property
    def constants(self):
        from optimus.engines.datatable.constants import Constants
        return Constants()

    @property
    def encoding(self):
        from optimus.engines.datatable.ml.encoding import Encoding
        return Encoding(self)

    def _create_buffer_df(self, input_cols, n):
        pass

    def _buffer_window(self, input_cols, lower_bound, upper_bound):
        return DatatableDataFrame(self.data[input_cols][lower_bound: upper_bound], op=self.op, label_encoder=self.le)

    def set_buffer(self, columns="*", n=None):
        return True

    def get_buffer(self):
        return self

    def to_optimus_pandas(self):
        return self.root

    def to_pandas(self):
        return self.root.data.to_pandas()
