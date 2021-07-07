from optimus.engines.base.commons.functions import is_integer, is_float, is_numeric, is_string
from optimus.engines.base.mask import Mask


class PandasMask(Mask):

    def str(self, cols="*"):
        return self.root[cols].cols.apply(cols, is_string) 

    def int(self, cols="*"):
        return self.root[cols].cols.apply(cols, is_integer)

    def float(self, cols="*"):
        return self.root[cols].cols.apply(cols, is_float)

    def numeric(self, cols="*"):
        return self.root[cols].cols.apply(cols, is_numeric)