from optimus.engines.cudf.extension import Ext as CUDFExtension
from optimus.engines.cudf.io.save import Save


class CUDFDataFrame(CUDFExtension):
    def __init__(self, data):
        super().__init__(self, data)

    @property
    def rows(self):
        from optimus.engines.cudf.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.cudf.columns import Cols
        return Cols(self)

    @property
    def save(self):
        return Save(self)

    @property
    def functions(self):
        from optimus.engines.cudf.functions import CUDFFunctions
        return CUDFFunctions()

    @property
    def constants(self):
        from optimus.engines.cudf.constants import constants
        return constants(self)