import time

from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import BUFFER_SIZE


def _set_buffer(odf, columns="*", n=BUFFER_SIZE):
    input_cols = parse_columns(odf, columns)

    odf.buffer = PandasDataFrame(odf.cols.select(input_cols).rows.limit(n).to_pandas())
    odf.meta.set("buffer_time", int(time.time()))


def _buffer_windows(odf, columns=None, lower_bound=None, upper_bound=None, n=BUFFER_SIZE):
    buffer_time = odf.meta.get("buffer_time")
    last_action_time = odf.meta.get("last_action_time")

    if buffer_time and last_action_time:
        if buffer_time > last_action_time:
            odf.set_buffer(columns, n)
    elif odf.get_buffer() is None:
        odf.set_buffer(columns, n)

    df_buffer = odf.get_buffer()
    df_length = df_buffer.rows.count()
    if lower_bound is None:
        lower_bound = 0

    if lower_bound < 0:
        lower_bound = 0

    if upper_bound is None:
        upper_bound = df_length

    if upper_bound > df_length:
        upper_bound = df_length

    if lower_bound >= df_length:
        diff = upper_bound - lower_bound
        lower_bound = df_length - diff
        upper_bound = df_length
        # RaiseIt.value_error(df_length, str(df_length - 1))

    input_columns = parse_columns(odf, columns)
    return PandasDataFrame(df_buffer.data[input_columns][lower_bound: upper_bound])