from optimus.engines.base.commons.functions import is_integer_cudf, is_float_cudf, is_numeric_cudf, is_string_cudf
from optimus.engines.base.mask import Mask


class CUDFMask(Mask):

    def numeric(self, cols="*"):
        return self.root.cols.apply(cols, is_numeric_cudf)

    def int(self, cols="*"):
        return self.root.cols.apply(cols, is_integer_cudf)

    def float(self, cols="*"):
        return self.root.cols.apply(cols, is_float_cudf)

    def str(self, cols="*"):
        return self.root.cols.apply(cols, is_string_cudf)