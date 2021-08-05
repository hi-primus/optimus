from optimus.engines.base.dataframe.rows import DataFrameBaseRows
from optimus.engines.base.pandas.rows import PandasBaseRows
from optimus.engines.base.rows import BaseRows


class Rows(DataFrameBaseRows, PandasBaseRows, BaseRows):
    pass
