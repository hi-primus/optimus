from optimus.engines.base.basedataframe import BaseDataFrame
from optimus.engines.base.dataframe.dataframe import DataFrameBaseDataFrame
from optimus.engines.cudf.dataframe import CUDFDataFrame
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.engines.vaex.io.save import Save
from optimus.helpers.converter import pandas_to_vaex_dataframe


class VaexDataFrame(DataFrameBaseDataFrame):

    def _base_to_dfd(self, pdf, n_partitions):
        return pandas_to_vaex_dataframe(pdf, n_partitions)

    def _iloc(self, lower_bound, upper_bound, copy=True):
        dfd = self.data[lower_bound: upper_bound]
        if copy:
            dfd = dfd.copy()
        return self.root.new(dfd, meta=self.root.meta)

    def _assign(self, kw_columns):
        dfd = self.root.data
        _dfd = dfd.copy()
        for col_name, expr in kw_columns.items():
            _dfd[col_name] = expr
        return _dfd

    def _sample(self, n=10, seed=False):
        return self.root.data.sample(n=n, random_state=seed)

    def visualize(self):
        pass

    def stratified_sample(self, col_name, seed: int = 1) -> 'DataFrameType':
        pass

    def graph(self) -> dict:
        pass

    def to_pandas(self):
        return self.data.to_pandas_df().reset_index(drop=True)

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
        from optimus.engines.vaex.functions import VaexFunctions
        return VaexFunctions(self)

    @property
    def mask(self):
        from optimus.engines.vaex.mask import VaexMask
        return VaexMask(self)

    @property
    def ml(self):
        from optimus.engines.pandas.ml.models import ML
        return ML(self)

    @property
    def encoding(self):
        from optimus.engines.pandas.ml.encoding import Encoding
        return Encoding(self)

    @property
    def constants(self):
        from optimus.engines.vaex.constants import Constants
        return Constants()

    def to_optimus_pandas(self):
        return PandasDataFrame(self.root.to_pandas(), op=self.op)

    def to_optimus_cudf(self):
        return CUDFDataFrame(self.root.to_pandas(), op=self.op)



    def partitions(self):
        pass

    @staticmethod
    def partitioner():
        pass
