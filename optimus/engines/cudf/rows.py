import functools
import operator
from optimus.engines.base.cudf.rows import CUDFBaseRows

import cudf
import pandas as pd
from optimus.engines.base.dataframe.rows import DataFrameBaseRows
from optimus.engines.base.cudf.rows import CUDFBaseRows
from optimus.engines.base.rows import BaseRows


class Rows(DataFrameBaseRows, CUDFBaseRows, BaseRows):
    pass