from optimus.engines.base.basedataframe import BaseDataFrame

from optimus.engines.cudf.dataframe import CUDFDataFrame
from optimus.engines.dask.io.save import Save
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.converter import pandas_to_vaex_dataframe


class VaexDataFrame(BaseDataFrame):

    def __init__(self, data):
        super().__init__(self, data)

    def _base_to_dfd(self, pdf, n_partitions):
        return pandas_to_vaex_dataframe(pdf, n_partitions)

    def _buffer_window(self, input_cols, lower_bound, upper_bound):
        pass

    def _assign(self, kw_columns):
        dfd = self.root.data
        for col_name, functions in kw_columns.items():
            dfd[col_name] = functions

        return dfd

    def to_pandas(self):
        return self.data.to_pandas_df()

    @staticmethod
    def pivot(index, column, values):
        pass

    @staticmethod
    def melt(id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
        pass

    @staticmethod
    def query(sql_expression):
        pass

    @staticmethod
    def debug():
        pass

    @staticmethod
    def create_id(column="id"):
        pass

    @property
    def rows(self):
        from optimus.engines.vaex.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.vaex.columns import Cols
        return Cols(self)

    @property
    def save(self):
        return Save(self)

    @property
    def functions(self):
        from optimus.engines.dask.functions import DaskFunctions
        return DaskFunctions()

    @property
    def mask(self):
        from optimus.engines.dask.mask import DaskMask
        return DaskMask(self)

    @property
    def ml(self):
        from optimus.engines.dask.ml.models import ML
        return ML(self)

    @property
    def encoding(self):
        from optimus.engines.pandas.ml.encoding import Encoding
        return Encoding()

    def to_optimus_pandas(self):
        return PandasDataFrame(self.root.to_pandas())

    def to_optimus_cudf(self):
        return CUDFDataFrame(self.root.to_pandas())

    def visualize(self):
        pass

    @staticmethod
    def execute():
        pass

    @staticmethod
    def compute():
        pass

    @staticmethod
    def sample(n=10, random=False):
        pass

    def partitions(self):
        pass

    @staticmethod
    def partitioner():
        pass
