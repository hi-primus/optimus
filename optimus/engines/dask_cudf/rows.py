from optimus.engines.base.dask.rows import DaskBaseRows
from optimus.engines.base.cudf.rows import CUDFBaseRows
from optimus.engines.base.rows import BaseRows


class Rows(DaskBaseRows, CUDFBaseRows, BaseRows):
    pass
