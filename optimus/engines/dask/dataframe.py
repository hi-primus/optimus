from optimus.engines.base.dask.dataframe import DaskBaseDataFrame
from optimus.engines.cudf.dataframe import CUDFDataFrame
from optimus.engines.dask.io.save import Save
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.converter import pandas_to_dask_dataframe


class DaskDataFrame(DaskBaseDataFrame):
    def __init__(self, data):
        super().__init__(data)

    def _base_to_dfd(self, pdf, n_partitions):
        return pandas_to_dask_dataframe(pdf, n_partitions)

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
        from optimus.engines.dask.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.dask.columns import Cols
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
