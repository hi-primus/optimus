import vaex

from optimus.engines.base.mask import Mask
from optimus.helpers.columns import parse_columns
from optimus.helpers.core import val_to_list


class VaexMask(Mask):

    def null(self, cols="*", how="any") -> 'MaskDataFrameType':

        """
        Find the rows that have null values
        :param how:
        :param cols:
        :return:
        """
        df = self.root
        dfd = self.root.data
        col = val_to_list(parse_columns(df, cols))
        col = col[0]
        # for col in cols:
        dfd[col] = dfd[col].isna()

        return self.root.new(self._to_frame(dfd[[col]]))

    def _to_frame(self, series):
        return series
