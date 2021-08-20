from optimus.engines.base.pandas.rows import PandasBaseRows
from optimus.engines.base.dask.rows import DaskBaseRows


class Rows(PandasBaseRows, DaskBaseRows):
    pass
