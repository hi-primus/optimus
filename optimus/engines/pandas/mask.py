import fastnumbers
import numpy as np
import pandas as pd

from optimus.engines.base.commons.functions import is_integer, is_float
from optimus.engines.base.mask import Mask


class PandasMask(Mask):

    def integer(self, col_name="*"):
        return self.root.cols.apply(col_name, is_integer)

    def float(self, col_name="*"):

        return self.root.cols.apply(col_name, is_float)


