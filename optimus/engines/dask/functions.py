# This functions must handle one or multiple columns
# Must return None if the data type can not be handle
from dask import delayed
from dask.array import stats

from optimus.engines.base.functions import op_delayed


def kurtosis(df, columns, args):
    # Maybe we could contribute with this
    # `nan_policy` other than 'propagate' have not been implemented.

    f = {col_name: stats.kurtosis(df[col_name].ext.to_float()) for col_name in columns}

    @op_delayed(df)
    def _kurtosis(_f):
        return {"kurtosis": _f}

    return _kurtosis(f)


def skewness(df, columns, args):
    f = {col_name: float(stats.skew(df[col_name].ext.to_float())) for col_name in columns}

    @op_delayed(df)
    def _skewness(_f):
        return {"skewness": _f}

    return _skewness(f)
