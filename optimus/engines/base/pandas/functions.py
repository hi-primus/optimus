
from optimus.helpers.logger import logger
import numpy as np
import pandas as pd
from fastnumbers import isintlike, isfloat, isreal, fast_forceint, fast_float

from optimus.infer import is_int_like, is_list_or_tuple

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
        if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
            return False
        if str(series.dtype) in self.constants.INT_INTERNAL_TYPES:
            return True
        return np.vectorize(isintlike)(series).flatten()

    @staticmethod
    def is_float(series):
        return np.vectorize(isfloat)(series).flatten()

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
            series = cls._partition_engine.Series(np.vectorize(fast_forceint,
                               otypes=otypes)(series, default=default,
                                              on_fail=lambda x: default).flatten())
                                              
        except Exception:
            series = series.replace([np.inf, -np.inf], default)
            if int_type:
                series = cls._partition_engine.Series(np.floor(cls._partition_engine.to_numeric(series, errors='coerce', downcast='integer'))).fillna(default)
                try:
                    series = series.astype('int64')
                except:
                    pass
            else:
                series = cls._partition_engine.Series(np.floor(cls._partition_engine.to_numeric(series, errors='coerce')))
                series = series if default is None else series.fillna(default)

        return series

    @classmethod
    def _to_float(cls, series):
        try:
            return cls._partition_engine.Series(np.vectorize(fast_float)(series, default=np.nan).flatten())
        except:
            return cls._partition_engine.Series(cls._partition_engine.to_numeric(series, errors='coerce')).astype('float')

    @classmethod
    def _to_datetime(cls, value, format=None):
        try:
            if format is not None:
                return cls._partition_engine.to_datetime(value, format=format, errors="coerce")
        except Exception as e:
            logger.warn(e)
        return cls._partition_engine.to_datetime(value, errors="coerce")

    @classmethod
    def format_date(cls, series, current_format=None, output_format=None):
        return cls._partition_engine.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format).reset_index(
            drop=True)

    @classmethod
    def td_between(cls, series, value=None, date_format=None):

        value_date_format = date_format

        if is_list_or_tuple(date_format) and len(date_format) == 2:
            date_format, value_date_format = date_format

        if is_list_or_tuple(value) and len(value) == 2:
            value, value_date_format = value

        date = cls._partition_engine.to_datetime(series, format=date_format, errors="coerce")
        value = pd.Timestamp.now() if value is None else cls._partition_engine.to_datetime(value, format=value_date_format, errors="coerce")

        return (value - date)
