from optimus.engines.base.dataframe.dataframe import DataFrameBaseDataFrame

from optimus.engines.pandas.io.save import Save


class PolarsDataFrame(DataFrameBaseDataFrame):

    @staticmethod
    def query(sql_expression):
        pass

    @staticmethod
    def debug():
        pass

    def visualize(self):
        pass

    def graph(self) -> dict:
        pass

    def _assign(self, kw_columns: dict):
        kw_columns = {str(key): kw_column for key, kw_column in kw_columns.items()}
        return self.root.data.assign(**kw_columns)

    def _base_to_dfd(self, pdf, n_partitions):
        pass

    @property
    def rows(self):
        from optimus.engines.polars.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.polars.columns import Cols
        print(self.root.data,"sdf")
        return Cols(self)

    @property
    def save(self):
        return Save(self)

    @property
    def functions(self):
        from optimus.engines.polars.functions import PolarsFunctions
        return PolarsFunctions(self)

    @property
    def mask(self):
        from optimus.engines.polars.mask import PolarsMask
        return PolarsMask(self)

    @property
    def ml(self):
        from optimus.engines.polars.ml.models import ML
        return ML(self)

    @property
    def constants(self):
        from optimus.engines.polars.constants import Constants
        return Constants()

    @property
    def encoding(self):
        from optimus.engines.polars.ml.encoding import Encoding
        return Encoding(self)

    def _iloc(self, lower_bound, upper_bound, copy=True):
        return self.root.new(self.data.collect()[lower_bound: upper_bound], meta=self.root.meta)

    def to_optimus_pandas(self):
        return self.root

    def to_pandas(self):
        print("ASADASDASD",self.root.data)
        return self.root.data.collect().to_pandas()
