from optimus.engines.base.commons.functions import is_string, is_integer, is_float, is_numeric
from optimus.engines.base.mask import Mask


class DaskMask(Mask):

    def str(self, col_name="*"):
        return self.root.cols.apply(col_name, is_string, mode="partitioned")

    def int(self, col_name="*"):
        return self.root.cols.apply(col_name, is_integer, mode="partitioned")

    def float(self, col_name="*"):
        return self.root.cols.apply(col_name, is_float, mode="partitioned")

    def numeric(self, col_name="*"):
        return self.root.cols.apply(col_name, is_numeric, mode="partitioned")
