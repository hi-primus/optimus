from optimus.engines.base.pandas.rows import PandasBaseRows
from optimus.engines.base.dask.rows import DaskBaseRows
from optimus.engines.base.rows import BaseRows


class Rows(PandasBaseRows, DaskBaseRows, BaseRows):
    pass
