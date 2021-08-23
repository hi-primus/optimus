import cudf

from optimus.engines.base.functions import BaseFunctions
from abc import ABC


class CUDFBaseFunctions(BaseFunctions, ABC):

    def to_dict(self, series) -> dict:
        series.name = str(series.name)
        return series.to_pandas().to_dict()

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
        return cudf.to_numeric(series, errors="ignore", downcast="float")

    @staticmethod
    def to_string(series):
        return series.astype(str)

    def _to_datetime(self, value, format):
        return cudf.to_datetime(value, format=format, errors="coerce")
