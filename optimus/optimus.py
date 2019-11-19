def optimus(engine="spark", *args, **kwargs):
    if engine == "spark":
        from optimus.engines.spark import SparkEngine
        return SparkEngine(*args, **kwargs)
    elif engine == "dask":
        from optimus.engines.dask import DaskEngine
        return DaskEngine(*args, **kwargs)
    elif engine == "dask_cudf":
        from optimus.engines.dask_cudf import DaskCUDFEngine
        return DaskCUDFEngine(*args, **kwargs)
    elif engine == "pandas":
        from optimus.engines.pandas import PandasEngine
        return PandasEngine(*args, **kwargs)
