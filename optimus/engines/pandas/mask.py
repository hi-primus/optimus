from optimus.engines.base.commons.functions import is_integer, is_float, is_numeric, is_string
from optimus.engines.base.mask import Mask


class PandasMask(Mask):

    def string(self, col_name="*"):
        return self.root.cols.apply(col_name, is_string)

    def integer(self, col_name="*"):
        return self.root.cols.apply(col_name, is_integer)

    def float(self, col_name="*"):
        return self.root.cols.apply(col_name, is_float)

    def numeric(self, col_name="*"):
        return self.root.cols.apply(col_name, is_numeric)