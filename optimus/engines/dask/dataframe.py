from optimus.engines.base.dask.extension import Ext as DaskExtension
from optimus.helpers.constants import BUFFER_SIZE
from optimus.engines.buffer import _set_buffer, _buffer_windows


class DaskDataFrame(DaskExtension):
    def __init__(self, data):
        super().__init__(self, data)

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
    def functions(self):
        from optimus.engines.dask.functions import DaskFunctions
        return DaskFunctions(self)

    @property
    def mask(self):
        from optimus.engines.base.mask import Mask
        return Mask(self)

    def set_buffer(self, columns="*", n=BUFFER_SIZE):
        return _set_buffer(self, columns=columns, n=n)

    def buffer_window(self, columns=None, lower_bound=None, upper_bound=None, n=BUFFER_SIZE):
        return _buffer_windows(self, columns=columns, lower_bound=lower_bound, upper_bound=upper_bound, n=n)