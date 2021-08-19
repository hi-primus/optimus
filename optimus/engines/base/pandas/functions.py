
import numpy as np
import pandas as pd
from fastnumbers import isintlike, isfloat, isreal, fast_int, fast_float

from optimus.infer import is_int_like

from optimus.engines.base.functions import BaseFunctions
from abc import ABC


class PandasBaseFunctions(BaseFunctions, ABC):

    @staticmethod
    def is_string(series):
        def _is_string(value):
            if isinstance(value, str):
                return True
            else:
                return False

        return np.vectorize(_is_string)(series.values).flatten()

    @staticmethod
    def is_integer(series):
        return np.vectorize(isintlike)(series).flatten()

    def is_float(self, series):
        return np.vectorize(isfloat)(series).flatten()

    @staticmethod
    def is_numeric(series):
        return np.vectorize(isreal)(series).flatten()

    def _to_integer(self, series, default=0):
        try:
            series = pd.Series(np.vectorize(fast_int)(series, default=default).flatten())
        except:
            if is_int_like(default):
                default = int(default)
                series = pd.Series(np.floor(pd.to_numeric(series, errors='coerce', downcast='integer'))).fillna(default)
                try:
                    series = series.astype('int64')
                except:
                    pass
            else:
                series = pd.Series(np.floor(pd.to_numeric(series, errors='coerce')))
                series = series if default is None else series.fillna(default)

        return series

    def _to_float(self, series):
        try:
            return pd.Series(np.vectorize(fast_float)(series, default=np.nan).flatten())
        except:
            return pd.Series(pd.to_numeric(series, errors='coerce')).astype('float')

    def _to_datetime(self, value, format=None):
        if format is None:
            return pd.to_datetime(value, errors="coerce")
        else:
            return pd.to_datetime(value, format=format, errors="coerce")
