
import numpy as np
import pandas as pd
from fastnumbers import isintlike, isfloat, isreal, fast_forceint, fast_float

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

    def is_integer(self, series):
        if str(series.dtype) in self.constants.DATETIME_TYPES:
            return False
        if str(series.dtype) in self.constants.INT_TYPES:
            return True
        return np.vectorize(isintlike)(series).flatten()

    @staticmethod
    def is_float(series):
        return np.vectorize(isfloat)(series).flatten()

    def is_numeric(self, series):
        if str(series.dtype) in self.constants.DATETIME_TYPES:
            return False
        return np.vectorize(isreal)(series).flatten()

    @staticmethod
    def _to_integer(series, default=0):

        # TODO replace_inf

        if is_int_like(default):
            int_type = True
            default = int(default)
            otypes = [int]
        else:
            int_type = False
            otypes = [object]

        try:
            if default is not None:
                series = series.fillna(default)
            series = pd.Series(np.vectorize(fast_forceint,
                               otypes=otypes)(series, default=default,
                                              on_fail=lambda x: default).flatten())
                                              
        except Exception:
            series = series.replace([np.inf, -np.inf], default)
            if int_type:
                series = pd.Series(np.floor(pd.to_numeric(series, errors='coerce', downcast='integer'))).fillna(default)
                try:
                    series = series.astype('int64')
                except:
                    pass
            else:
                series = pd.Series(np.floor(pd.to_numeric(series, errors='coerce')))
                series = series if default is None else series.fillna(default)

        return series

    @staticmethod
    def _to_float(series):
        try:
            return pd.Series(np.vectorize(fast_float)(series, default=np.nan).flatten())
        except:
            return pd.Series(pd.to_numeric(series, errors='coerce')).astype('float')

    @staticmethod
    def _to_datetime(value, format=None):
        if format is None:
            return pd.to_datetime(value, errors="coerce")
        else:
            return pd.to_datetime(value, format=format, errors="coerce")
