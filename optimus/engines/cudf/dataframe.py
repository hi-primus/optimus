from optimus.engines.base.dataframe.dataframe import Ext as BaseDataFrame
from optimus.engines.cudf.io.save import Save


class CUDFDataFrame(BaseDataFrame):
    def __init__(self, data):
        super().__init__(self, data)

    @property
    def rows(self):
        from optimus.engines.cudf.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.cudf.columns import Cols
        return Cols(self)

    @property
    def save(self):
        return Save(self)

    @property
    def functions(self):
        from optimus.engines.cudf.functions import CUDFFunctions
        return CUDFFunctions()

    @property
    def constants(self):
        from optimus.engines.cudf.constants import constants
        return constants(self)

    def head(self, columns="*", n=10):
        """

        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)
        return df.data[columns].head(n).to_pandas()

    def to_pandas(self):
        return self.data.to_pandas()

    def to_dict(self, orient="records", index=True):
        """
        Create a dict
        :param orient:
        :param index: Return the series index
        :return:
        """

        series = self.data
        if index is True:
            return series.to_pandas().to_dict(orient)
        else:
            return series.to_pandas().to_list()

    def repartition(self, n=None, *args, **kwargs):
        return self.root
