
def optimus(engine="spark", *args, **kwargs):
    if engine == "spark":
        from optimus.engines.spark import SparkEngine
        return SparkEngine(*args, **kwargs)
    elif engine == "dask":
        return DaskEngine(*args, **kwargs)
    elif engine == "dask_cudf":
        from optimus.engines.daskcudf.engine import DaskCUDFEngine
        return DaskCUDFEngine(*args, **kwargs)
    elif engine == "pandas":
        from optimus.engines.pandas import PandasEngine
        return PandasEngine(*args, **kwargs)
