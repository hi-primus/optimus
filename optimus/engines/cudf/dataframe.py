from optimus.engines.base.dataframe.dataframe import DataFrameBaseDataFrame
from optimus.engines.base.basedataframe import BaseDataFrame
from optimus.engines.cudf.io.save import Save
from optimus.engines.pandas.dataframe import PandasDataFrame


class CUDFDataFrame(DataFrameBaseDataFrame, BaseDataFrame):

    def __init__(self, data):
        super().__init__(data)

    def _base_to_dfd(self, pdf, n_partitions):
        pass

    def to_optimus_pandas(self):
        return PandasDataFrame(self.root.data.to_pandas())

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
    def mask(self):
        from optimus.engines.cudf.mask import CUDFMask
        return CUDFMask(self)

    @property
    def functions(self):
        from optimus.engines.cudf.functions import CUDFFunctions
        return CUDFFunctions()

    @property
    def constants(self):
        from optimus.engines.cudf.constants import constants
        return constants(self)

    def _buffer_window(self, input_cols, lower_bound, upper_bound):
        return PandasDataFrame(self.data[input_cols][lower_bound: upper_bound].to_pandas())

    def encoding(self):
        pass

    def get_buffer(self):
        return self

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
