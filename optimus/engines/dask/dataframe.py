from optimus.engines.base.dask.dataframe import DaskBaseDataFrame
from optimus.engines.buffer import _set_buffer, _buffer_windows
from optimus.engines.cudf.dataframe import CUDFDataFrame
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.engines.dask.io.save import Save
from optimus.helpers.constants import BUFFER_SIZE


class DaskDataFrame(DaskBaseDataFrame):
    def __init__(self, root, data):
        super().__init__(root, data)

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

    def set_buffer(self, columns="*", n=BUFFER_SIZE):
        return _set_buffer(self, columns=columns, n=n)

    def buffer_window(self, columns=None, lower_bound=None, upper_bound=None, n=BUFFER_SIZE):
        return _buffer_windows(self, columns=columns, lower_bound=lower_bound, upper_bound=upper_bound, n=n)

    def to_optimus_pandas(self):
        return PandasDataFrame(self.root.to_pandas())

    def to_optimus_cudf(self):
        return CUDFDataFrame(self.root.to_pandas())
