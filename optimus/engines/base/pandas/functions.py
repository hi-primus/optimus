import numpy as np
import pandas as pd

from fastnumbers import isintlike, isfloat, isreal, fast_int, fast_float

class PandasBaseFunctions():

    
    def is_string(self, series):
        def _is_string(value):
            if isinstance(value, str):
                return True
            else:
                return False

        return pd.Series(np.vectorize(_is_string)(series.values).flatten())


    def is_integer(self, series):
        return pd.Series(np.vectorize(isintlike)(series).flatten())


    def is_float(self, series):
        return pd.Series(np.vectorize(isfloat)(series).flatten())


    def is_numeric(self, series):
        return pd.Series(np.vectorize(isreal)(series).flatten())\

    def to_integer(self, series, *args):
        try:
            return pd.Series(np.vectorize(fast_int)(series, default=np.nan).flatten())
        except TypeError:
            return pd.Series(pd.to_numeric(series, errors='coerce')).astype('int')


    def to_float(self, series, *args):
        try:
            return pd.Series(np.vectorize(fast_float)(series, default=np.nan).flatten())
        except:
            return pd.Series(pd.to_numeric(series, errors='coerce')).astype('float')

    def to_string(self, value, *args):
        try:
            return value.astype(str)
        except TypeError:
            return np.nan

    def to_datetime(self, value, format):
        return pd.to_datetime(value, format=format, errors="coerce")
   