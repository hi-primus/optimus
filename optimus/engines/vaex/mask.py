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
        cols = val_to_list(parse_columns(df, cols))
        # subset_df = dfd[cols]

        for col in cols:
            mask = dfd[col].isna()


        # if how == "all":
        #     col_name = cols[0] if len(cols) == 1 else "__null__"
        #     mask = subset_df.isnull().all(axis=1).rename(col_name)
        # else:
        #     mask = subset_df.isnull()

        return self.root.new(self._to_frame(mask))
