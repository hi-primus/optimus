from abc import ABC

import cudf

from optimus.engines.base.functions import BaseFunctions


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
        return series.str.isinteger()

    def is_float(self, series):
        return series.str.isfloat()

    def is_numeric(self, series):
        return series.str.isnumeric()

    def is_string(self, series):
        return series.str.isalpha()

    def _to_integer(self, series):
        return cudf.to_numeric(series, errors="ignore", downcast="integer")

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
