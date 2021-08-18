import numpy as np
import pandas as pd
from fastnumbers import isintlike, isfloat, isreal

from optimus.engines.base.functions import BaseFunctions


class SparkBaseFunctions(BaseFunctions):

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
        return pd.Series(np.vectorize(isreal)(series).flatten())

    def _to_integer(self, series, default=0):
        return series.astype(int)

    def _to_float(self, series):
        return series.astype(float)

    def to_string(self, value):
        try:
            return value.astype(str)
        except TypeError:
            return np.nan

    def _to_datetime(self, value, format=None):
        if format is None:
            return pd.to_datetime(value, errors="coerce")
        else:
            return pd.to_datetime(value, format=format, errors="coerce")
