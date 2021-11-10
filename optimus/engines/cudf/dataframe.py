from optimus.engines.base.cudf.dataframe import CUDFBaseDataFrame
from optimus.engines.base.dataframe.dataframe import DataFrameBaseDataFrame
from optimus.engines.cudf.io.save import Save
from optimus.engines.pandas.dataframe import PandasDataFrame


class CUDFDataFrame(CUDFBaseDataFrame, DataFrameBaseDataFrame):

    def _base_to_dfd(self, pdf, n_partitions):
        pass

    def to_optimus_pandas(self):
        return PandasDataFrame(self.root.data.to_pandas(), op=self.op)

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
        return CUDFFunctions(self)

    @property
    def constants(self):
        from optimus.engines.cudf.constants import Constants
        return Constants()

    def _buffer_window(self, input_cols, lower_bound, upper_bound):
        return PandasDataFrame(self.data[input_cols][lower_bound: upper_bound].to_pandas().reset_index(drop=True), op=self.op, label_encoder=self.le)

    def get_buffer(self):
        return self

    def to_pandas(self):
        return self.data.to_pandas()

    def to_dict(self, cols="*", n=10, orient="list"):
        """
        Create a dict
        :param cols:
        :param n: number of rows
        :param orient:
        :return:
        """

        if n == "all":
            series = self.cols.select(cols).to_pandas()
        else:
            series = self.buffer_window(cols, 0, n).to_pandas()

        return series.to_dict(orient)

    def repartition(self, n=None, *args, **kwargs):
        return self.root
