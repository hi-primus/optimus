import numpy as np
import pandas as pd


from optimus.engines.base.pandas.functions import PandasBaseFunctions
from optimus.engines.base.dataframe.functions import DataFrameBaseFunctions


class PandasFunctions(PandasBaseFunctions, DataFrameBaseFunctions):

    _engine = pd

    @staticmethod
    def dask_to_compatible(dfd):
        from optimus.helpers.converter import dask_dataframe_to_pandas
        return dask_dataframe_to_pandas(dfd)

    @staticmethod
    def df_concat(df_list):
        return pd.concat(df_list, axis=0, ignore_index=True)

    @staticmethod
    def new_df(*args, **kwargs):
        return pd.DataFrame(*args, **kwargs)

    def count_zeros(self, series, *args):
        return int((self.to_float(series).values == 0).sum())

    def kurtosis(self, series):
        # use scipy to match function from dask.array.stats
        from scipy.stats import kurtosis
        return kurtosis(self.to_float(series.dropna()))

    def skew(self, series):
        # use scipy to match function from dask.array.stats
        from scipy.stats import skew
        return skew(self.to_float(series.dropna()))

    def exp(self, series):
        return np.exp(self.to_float(series))

    def sqrt(self, series):
        return np.sqrt(self.to_float(series))

    def reciprocal(self, series):
        return np.reciprocal(self.to_float(series))

    def radians(self, series):
        return np.radians(self.to_float(series))

    def degrees(self, series):
        return np.degrees(self.to_float(series))

    def ln(self, series):
        return np.log(self.to_float(series))

    def log(self, series, base=10):
        return np.log(self.to_float(series)) / np.log(base)

    def sin(self, series):
        return np.sin(self.to_float(series))

    def cos(self, series):
        return np.cos(self.to_float(series))

    def tan(self, series):
        return np.tan(self.to_float(series))

    def asin(self, series):
        return np.arcsin(self.to_float(series))

    def acos(self, series):
        return np.arccos(self.to_float(series))

    def atan(self, series):
        return np.arctan(self.to_float(series))

    def sinh(self, series):
        return np.arcsinh(self.to_float(series))

    def cosh(self, series):
        return np.cosh(self.to_float(series))

    def tanh(self, series):
        return np.tanh(self.to_float(series))

    def asinh(self, series):
        return np.arcsinh(self.to_float(series))

    def acosh(self, series):
        return np.arccosh(self.to_float(series))

    def atanh(self, series):
        return np.arctanh(self.to_float(series))

    def floor(self, series):
        return np.floor(self.to_float(series))

    def ceil(self, series):
        return np.ceil(self.to_float(series))
        
    def normalize_chars(self, series):
        return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8')
