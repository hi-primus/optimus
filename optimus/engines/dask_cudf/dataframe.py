from optimus.engines.base.dask.dataframe import DaskBaseDataFrame
from optimus.engines.dask_cudf.io.save import Save
from optimus.engines.buffer import _set_buffer, _buffer_windows
from optimus.engines.cudf.dataframe import CUDFDataFrame
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import BUFFER_SIZE


class DaskCUDFDataFrame(DaskBaseDataFrame):

    def __init__(self, data):
        super().__init__(self, data)

    @property
    def rows(self):
        from optimus.engines.dask_cudf.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.dask_cudf.columns import Cols
        return Cols(self)

    @property
    def functions(self):
        from optimus.engines.dask_cudf.functions import DaskCUDFFunctions
        return DaskCUDFFunctions()

    @property
    def save(self):
        return Save(self)

    @property
    def mask(self):
        from optimus.engines.base.mask import Mask
        return Mask(self)

    @property
    def constants(self):
        from optimus.engines.base.dask.constants import constants
        return constants(self)

    def set_buffer(self, columns="*", n=BUFFER_SIZE):
        return _set_buffer(self, columns=columns, n=n)

    def buffer_window(self, columns=None, lower_bound=None, upper_bound=None, n=BUFFER_SIZE):
        return _buffer_windows(self, columns=columns, lower_bound=lower_bound, upper_bound=upper_bound, n=n)

    def head(self, columns="*", n=10):
        """

        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)
        return df.data[columns].head(n, npartitions=-1).to_pandas()

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

    def to_pandas(self):
        return self.data.compute().to_pandas()

    def to_optimus_pandas(self):
        return PandasDataFrame(self.root.to_pandas())

    def to_optimus_cudf(self):
        return CUDFDataFrame(self.root.to_pandas())


    def to_dict(self, orient="records", limit=None):
        """
        Create a dict
        :param orient:
        :param limit:
        :return:
        """
        series = self.root
        return series.compute().to_pandas().to_dict(orient)