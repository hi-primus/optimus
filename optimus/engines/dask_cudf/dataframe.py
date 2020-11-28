from optimus.engines.dask_cudf.extension import Ext as DaskCUDFExtension


class DaskCUDFDataFrame(DaskCUDFExtension):
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
        return DaskCUDFFunctions(self)

    @property
    def meta(self):
        return Meta(self)