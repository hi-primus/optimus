import pandas as pd
import dask.dataframe as dd

from optimus.engines.base.dask.dataframe import DaskBaseDataFrame
from optimus.engines.dask.io.save import Save
from optimus.engines.base.pandas.dataframe import PandasBaseDataFrame
from optimus.helpers.converter import pandas_to_dask_dataframe


class DaskDataFrame(PandasBaseDataFrame, DaskBaseDataFrame):

    @staticmethod
    def _compatible_data(data):
        if isinstance(data, (pd.DataFrame, pd.Series)):
            data = pandas_to_dask_dataframe(data)
        
        if isinstance(data, dd.Series):
            data = data.to_frame()

        return data

    def _base_to_dfd(self, pdf, n_partitions):
        return pandas_to_dask_dataframe(pdf, n_partitions)

    @staticmethod
    def melt(id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
        pass

    @staticmethod
    def query(sql_expression):
        pass

    @staticmethod
    def debug():
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
        return DaskFunctions(self)

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
        from optimus.engines.dask.ml.encoding import Encoding
        return Encoding(self)

    def to_optimus_pandas(self):
        from optimus.engines.pandas.dataframe import PandasDataFrame
        return PandasDataFrame(self.root.to_pandas(), op=self.op)

    def to_optimus_cudf(self):
        from optimus.engines.cudf.dataframe import CUDFDataFrame
        return CUDFDataFrame(self.root.to_pandas(), op=self.op)
