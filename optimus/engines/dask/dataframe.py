from optimus.engines.base.dask.dataframe import DaskBaseDataFrame
from optimus.engines.cudf.dataframe import CUDFDataFrame
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.engines.dask.io.save import Save
from optimus.helpers.constants import BUFFER_SIZE
from optimus.helpers.converter import pandas_to_dask_dataframe


class DaskDataFrame(DaskBaseDataFrame):
    def __init__(self,  data):
        super().__init__(self, data)

    @staticmethod
    def _pandas_to_dfd(pdf, npartitions):
        return pandas_to_dask_dataframe(pdf, npartitions)


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
        from optimus.engines.base.mask import Mask
        return Mask(self)

    def to_optimus_pandas(self):
        return PandasDataFrame(self.root.to_pandas())

    def to_optimus_cudf(self):
        return CUDFDataFrame(self.root.to_pandas())
