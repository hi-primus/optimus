import numpy as np

from optimus.infer import is_dict, is_dict_of_one_element, is_list_value, is_list_of_one_element


def tuple_to_dict(value):
    """
    Convert tuple to dict
    :param value: tuple to be converted
    :return:
    """

    return dict((x, y) for x, y in value)


def format_dict(_dict, tidy=True):
    """
    This function format a dict. If the main dict or a deep dict has only on element
     {"col_name":{0.5: 200}} we get 200
    :param _dict: dict to be formatted
    :param tidy:
    :return:
    """

    if tidy is True:
        levels = 2
        while (levels>=0):
            levels -= 1
            if is_list_of_one_element(_dict):
                _dict = _dict[0]
            elif is_dict_of_one_element(_dict):
                _dict = next(iter(_dict.values()))
            else:
                return _dict
                
    else:
        if is_list_of_one_element(_dict):
            return _dict[0]
        else:
            return _dict

def convert_numpy(value):
    if isinstance(value, (dict,)):
        for key in value:
            value[key] = convert_numpy(value[key])
        return value
    elif isinstance(value, (list, set, tuple)):
        return value.__class__(map(convert_numpy, value))
    elif isinstance(value, (np.generic,)):
        return np.asscalar(value)
    elif hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    else:
        return value

# def cudf_series_to_pandas(serie):
#     return serie.to_pandas()

def dask_dataframe_to_dask_cudf(df):
    import cudf
    return df.map_partitions(cudf.DataFrame.from_pandas)

# To cudf
def dask_dataframe_to_cudf(df):
    return pandas_to_cudf(dask_dataframe_to_pandas(df))

# def dask_cudf_to_cudf(df):
#     return df.compute()


# To Pandas
def spark_to_pandas(df):
    return df.toPandas()


def dask_cudf_to_pandas(df):
    return df.map_partitions(lambda df: df.to_pandas())


def dask_dataframe_to_pandas(df):
    return df.compute()


def cudf_to_pandas(df):
    return df.to_pandas()

def cudf_to_dask_cudf(df, n_partitions=1):
    import dask_cudf
    return dask_cudf.from_cudf(df, npartitions=1)

# def cudf_to_cupy_arr(df):
#     import cupy as cp
#     return cp.fromDlpack(df.to_dlpack())

def pandas_to_cudf(df):
    import cudf
    return cudf.from_pandas(df)


def pandas_to_dask_dataframe(pdf, n_partitions=1):
    from dask import dataframe as dd
    return dd.from_pandas(pdf, npartitions=n_partitions)


def pandas_to_vaex_dataframe(pdf, n_partitions=1):
    import vaex
    return vaex.from_pandas(pdf)


def pandas_to_dask_cudf_dataframe(pdf, n_partitions=1):
    import cudf
    import dask_cudf
    # Seems that from_cudf also accepts pandas
    cdf = cudf.DataFrame.from_pandas(pdf)
    return dask_cudf.from_cudf(cdf, npartitions=n_partitions)

def pandas_to_spark_dataframe(pdf, n_partitions=1):
    import databricks.koalas as ks
    return ks.from_pandas(pdf)
