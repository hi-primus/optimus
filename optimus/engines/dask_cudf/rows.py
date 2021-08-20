from optimus.engines.base.cudf.rows import CUDFBaseRows
from optimus.engines.base.dask.rows import DaskBaseRows


class Rows(CUDFBaseRows, DaskBaseRows):
    pass
