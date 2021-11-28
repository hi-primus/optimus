from datatable import ifelse, f
from optimus.engines.base.mask import Mask
from optimus.helpers.columns import parse_columns


class DatatableMask(Mask):
    def numeric(self, cols="*"):
        cols = parse_columns(self.root, cols)
        return self.root[cols].cols.apply(cols, self.F.is_numeric, mode="vectorized")

    def empty(self, cols="*"):
        df = self.root
        mask = df.data[:, ifelse(f[cols] == None, True, False)]
        return self.root.new(self._to_frame(mask))

    def null(self, cols="*", how="any") -> 'MaskDataFrameType':
        """
        Find the rows that have null values
        :param how:
        :param cols:
        :return:
        """
        df = self.root
        cols = parse_columns(df, cols)
        subset_df = df.cols.select(cols).data

        if how == "all":
            col_name = cols[0] if len(cols) == 1 else "__null__"
            mask = subset_df.isnull().all(axis=1).rename(col_name)
        else:
            mask = df.data[:, ifelse(f[cols] == None, True, False)]

        print(mask)
        # print(df.data)
        return self.root.new(self._to_frame(mask))
