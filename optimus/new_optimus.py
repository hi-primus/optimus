from optimus.engines.base.dataframe.extension import Ext as PandasExtension
from optimus.engines.base.meta import Meta


# from optimus.engines.base.odataframe import BaseDataFrame


class PandasDataFrame(PandasExtension):
    def __init__(self, data):
        super().__init__(self, data)

    def new(self, df, meta=None):
        new_df = PandasDataFrame(df)
        if meta is not None:
            new_df.meta.set(value=meta.meta.get())
        return new_df

    @property
    def rows(self):
        from optimus.engines.pandas.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.pandas.columns import Cols
        return Cols(self)

    @property
    def functions(self):
        from optimus.engines.pandas.functions import PandasFunctions
        return PandasFunctions(self)


from optimus.engines.cudf.extension import Ext as CUDFExtension


class CUDFDataFrame(CUDFExtension):
    def __init__(self, data):
        super().__init__(self, data)

    def new(self, df, meta=None):
        new_df = CUDFDataFrame(df)
        if meta is not None:
            new_df.meta.set(value=meta.meta.get())
        return new_df

    @property
    def rows(self):
        from optimus.engines.cudf.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.cudf.columns import Cols
        return Cols(self)

    @property
    def functions(self):
        from optimus.engines.cudf.functions import CUDFFunctions
        return CUDFFunctions(self)


from optimus.engines.base.dask.extension import Ext as DaskExtension


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

    def new(self, df, meta=None):
        new_df = DaskDataFrame(df)
        if meta is not None:
            new_df.meta.set(value=meta.meta.get())
        return new_df

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


from optimus.engines.dask_cudf.extension import Ext as DaskCUDFExtension


class DaskCUDFDataFrame(DaskCUDFExtension):
    def __init__(self, data):
        super().__init__(self, data)

    def new(self, df, meta=None):
        new_df = DaskCUDFDataFrame(df)
        if meta is not None:
            new_df.meta.set(value=meta.meta.get())
        return new_df

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
        return DaskCUDFFunctions(self)

    @property
    def meta(self):
        return Meta(self)


class SparkDataFrame:
    def __init__(self, df):
        super().__init__(df)

    def new(self, df, meta=None):
        new_df = SparkDataFrame(df)
        if meta is not None:
            new_df.meta.set(value=meta.meta.get())
        return new_df

    @property
    def rows(self):
        from optimus.engines.spark.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.spark.columns import Cols
        return Cols(self)

    @property
    def constants(self):
        from optimus.engines.spark.constants import Constants
        return Constants()

    @property
    def functions(self):
        from optimus.engines.spark.functions import SparkFunctions
        return SparkFunctions(self)

    @property
    def meta(self):
        return Meta(self)


class IbisDataFrame:
    def __init__(self, df):
        super().__init__(df)

    def new(self, df):
        return IbisDataFrame(df)

    @property
    def rows(self):
        from optimus.engines.ibis.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.ibis.columns import Cols
        return Cols(self)

    @property
    def functions(self):
        from optimus.engines.ibis.functions import IbisFunctions
        return IbisFunctions(self)

    @property
    def meta(self):
        return Meta(self)
