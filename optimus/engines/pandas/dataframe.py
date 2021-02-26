from optimus.engines.base.dataframe.dataframe import Ext as BaseDataFrame
# from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.engines.pandas.io.save import Save
from optimus.helpers.columns import parse_columns
from optimus.helpers.converter import pandas_to_dask_dataframe


class PandasDataFrame(BaseDataFrame):
    def __init__(self, data):
        super().__init__(self, data)

    
    def _assign(self, kw_columns):
        return self.root.data.assign(**kw_columns)

    @property
    def rows(self):
        from optimus.engines.pandas.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.pandas.columns import Cols
        return Cols(self)

    @property
    def save(self):
        return Save(self)

    @property
    def functions(self):
        from optimus.engines.pandas.functions import PandasFunctions
        return PandasFunctions()

    @property
    def mask(self):
        from optimus.engines.pandas.mask import PandasMask
        return PandasMask(self)

    @property
    def constants(self):
        from optimus.engines.pandas.constants import constants
        return constants(self)

    def _create_buffer_df(self, input_cols, n):
        pass

    def _buffer_window(self, input_cols, lower_bound, upper_bound):
        return PandasDataFrame(self.data[input_cols][lower_bound: upper_bound])

    def set_buffer(self, columns="*", n=None):
        return True

    def get_buffer(self):
        return self

    def buffer_window(self, columns=None, lower_bound=None, upper_bound=None, n=None):

        if lower_bound is None:
            lower_bound = 0

        if lower_bound < 0:
            lower_bound = 0

        df_length = self.rows.count()

        if upper_bound is None:
            upper_bound = df_length

        input_columns = parse_columns(self, columns)

        if lower_bound == 0 and upper_bound == df_length:
            result = self[input_columns]
        else:
            if upper_bound > df_length:
                upper_bound = df_length

            if lower_bound >= df_length:
                diff = upper_bound - lower_bound
                lower_bound = df_length - diff
                upper_bound = df_length

            result = self._buffer_window(input_columns, lower_bound, upper_bound)

        return result

    def to_optimus_pandas(self):
        return self.root

    # def to_optimus_dask(self):
    #     df = self.root
    #     ddf = DaskDataFrame(pandas_to_dask_dataframe(self.root.data))
    #     ddf.meta = df.meta
    #     return ddf

    def to_pandas(self):
        return self.root.data
