from optimus.engines.base.commons.functions import is_string, is_integer, is_float, is_numeric
from optimus.engines.base.mask import Mask


class DaskMask(Mask):

    def str(self, cols="*"):
        return self.root.cols.apply(cols, is_string, mode="partitioned")

    def int(self, cols="*"):
        return self.root.cols.apply(cols, is_integer, mode="partitioned")

    def float(self, cols="*"):
        return self.root.cols.apply(cols, is_float, mode="partitioned")

    def numeric(self, cols="*"):
        return self.root.cols.apply(cols, is_numeric, mode="partitioned")
