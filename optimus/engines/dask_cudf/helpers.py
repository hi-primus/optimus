import cudf


def dask_pandas_to_dask_cudf(df):
    return df.map_partitions(cudf.DataFrame.from_pandas)
