import time

from optimus.engines.base.meta import Meta
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import BUFFER_SIZE


def _set_buffer(df, columns="*", n=BUFFER_SIZE):
    input_cols = parse_columns(df, columns)

    df_length = df.rows.count()

    if n > df_length:
        n = df_length
    
    df.buffer = PandasDataFrame(df.cols.select(input_cols).head(n=n))
    Meta.set(df.meta, "buffer_time", int(time.time()))


def _buffer_windows(df, columns=None, lower_bound=None, upper_bound=None, n=BUFFER_SIZE):
    meta = df.meta
    buffer_time = Meta.get(meta, "buffer_time")
    last_action_time = Meta.get(meta, "last_action_time")

    if buffer_time and last_action_time:
        if buffer_time > last_action_time:
            df.set_buffer(columns, n)

    df_buffer = df.get_buffer()

    if df_buffer is None:
        df.set_buffer(columns, n)
        df_buffer = df.get_buffer()

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

    input_columns = parse_columns(df, columns)
    return PandasDataFrame(df_buffer.data[input_columns][lower_bound: upper_bound])
