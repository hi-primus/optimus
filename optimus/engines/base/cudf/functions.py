import cudf


class CUDFBaseFunctions():

    def is_integer(self, series):
        return series.str.isinteger()

    def is_float(self, series):
        return series.str.isfloat()

    def is_numeric(self, series):
        return series.str.isnumeric()

    def is_string(self, series):
        return series.str.isalpha()

    def to_integer(self, series):
        return cudf.to_numeric(series, errors="ignore", downcast="integer")

    def to_float(self, series):
        return cudf.to_numeric(series, errors="ignore", downcast="float")

    def to_string(self, series):
        return series.astype(str)

    def to_datetime(self, value, format):
        return cudf.to_datetime(value, format=format, errors="coerce")

   