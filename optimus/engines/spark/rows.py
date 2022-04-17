# Helpers
from optimus.engines.base.dataframe.rows import DataFrameBaseRows
from optimus.engines.base.pandas.rows import PandasBaseRows
from optimus.engines.base.rows import BaseRows


# from optimus.infer_spark import is_list_of_spark_dataframes


class Rows(DataFrameBaseRows, PandasBaseRows, BaseRows):
    pass
