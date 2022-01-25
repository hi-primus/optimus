import re
from abc import ABC

import cudf

from optimus.engines.base.functions import BaseFunctions
from optimus.helpers.core import val_to_list


class CUDFBaseFunctions(BaseFunctions, ABC):
    @staticmethod
    def to_dict(series) -> dict:
        series.name = str(series.name)
        return series.to_pandas().to_dict()

    def to_items(self, series) -> dict:
        """
        Convert series to a list of tuples [(index, value), ...]
        """
        df = series.reset_index()
        return df.to_pandas().to_dict(orient='split')['data']

    def is_integer(self, series):
        if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
            return cudf.Series([False] * len(series))
        if str(series.dtype) in self.constants.INT_INTERNAL_TYPES:
            return cudf.Series([True] * len(series))
        return series.astype(str).str.isinteger()

    def is_float(self, series):
        if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
            return cudf.Series([False] * len(series))
        return series.astype(str).str.isfloat()

    def is_numeric(self, series):
        if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
            return cudf.Series([False] * len(series))
        return series.astype(str).str.isfloat()

    def is_string(self, series):
        if str(series.dtype) in [*self.constants.STRING_INTERNAL_TYPES, *self.constants.OBJECT_INTERNAL_TYPES]:
            return cudf.Series([True] * len(series))
        return cudf.Series([False] * len(series))

    def _to_integer(self, series, default=0):
        return cudf.to_numeric(series, errors="coerce", downcast="integer")

    def _to_float(self, series):
        import numpy as np
        # Workaround to https://github.com/rapidsai/cudf/issues/10049
        if series.dtype.type == np.bool_:
            return series.astype(float)
        else:
            return cudf.to_numeric(series, errors="coerce", downcast="float")

    @staticmethod
    def to_string(series):
        return series.astype(str)

    def _to_datetime(self, value, format):
        return cudf.to_datetime(value, format=format, errors="coerce")

    def replace_chars(self, series, search, replace_by, ignore_case):
        search = val_to_list(search, convert_tuple=True)
        if ignore_case:
            str_regex = [r'(?i)%s' % re.escape(s) for s in search]
        else:
            str_regex = [r'%s' % re.escape(s) for s in search]
        return self.to_string(series).str.replace(str_regex, replace_by, regex=True)

    def contains(self, series, value, case, flags, na, regex):
        return self.to_string_accessor(series).contains(value, case=case, flags=flags, regex=regex).fillna(na)
