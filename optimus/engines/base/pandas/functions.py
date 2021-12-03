from abc import ABC

import numpy as np
import pandas as pd
from fastnumbers import isintlike, isreal, fast_forceint, fast_float

from optimus.engines.base.functions import BaseFunctions
from optimus.helpers.logger import logger
from optimus.infer import is_int_like, is_list_or_tuple


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
        if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
            return False
        if str(series.dtype) in self.constants.INT_INTERNAL_TYPES:
            return True
        return np.vectorize(isintlike)(series).flatten()

    def is_float(self, series):
        if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
            return False
        # use isreal to allow strings like "0"
        return np.vectorize(isreal)(series).flatten()

    def is_numeric(self, series):
        if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
            return False
        return np.vectorize(isreal)(series).flatten()

    @classmethod
    def _to_integer(cls, series, default=0):

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

    @classmethod
    def _to_float(cls, series):
        try:
            return pd.Series(np.vectorize(fast_float)(series, default=np.nan).flatten())
        except:
            return pd.Series(pd.to_numeric(series, errors='coerce')).astype('float')

    @classmethod
    def _to_datetime(cls, value, format=None):
        try:
            if format is not None:
                return pd.to_datetime(value, format=format, errors="coerce")
        except Exception as e:
            logger.warn(e)
        return pd.to_datetime(value, errors="coerce")

    @classmethod
    def format_date(cls, series, current_format=None, output_format=None):
        return pd.to_datetime(series, format=current_format,
                              errors="coerce").dt.strftime(output_format).reset_index(drop=True)

    @classmethod
    def time_between(cls, series, value=None, date_format=None):

        value_date_format = date_format

        if is_list_or_tuple(date_format) and len(date_format) == 2:
            date_format, value_date_format = date_format

        if is_list_or_tuple(value) and len(value) == 2:
            value, value_date_format = value

        date = pd.to_datetime(series, format=date_format, errors="coerce", utc=True)

        if value is None:
            value = pd.Timestamp.now('utc')
        else:
            value = pd.to_datetime(value, format=value_date_format, errors="coerce", utc=True)

        return (value - date)
