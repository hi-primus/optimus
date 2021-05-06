import fastnumbers
import numpy as np
import pandas as pd

from optimus.engines.base.commons.functions import is_integer, is_float, is_integer_cudf, is_float_cudf, \
    is_numeric_cudf, is_string_cudf
from optimus.engines.base.mask import Mask


class CUDFMask(Mask):

    def numeric(self, col_name):
        return self.root.cols.apply(col_name, is_numeric_cudf)

    def integer(self, col_name="*"):
        return self.root.cols.apply(col_name, is_integer_cudf)

    def float(self, col_name="*"):
        return self.root.cols.apply(col_name, is_float_cudf)

    def string(self, col_name="*"):
        return self.root.cols.apply(col_name, is_string_cudf)