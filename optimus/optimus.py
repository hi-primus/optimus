def optimus(engine, *args, **kwargs):
    if engine == "spark":
        from optimus.engines.spark import SparkEngine
        return SparkEngine(*args, **kwargs)
    elif engine == "dask":
        from optimus.engines.dask import DaskEngine
        return DaskEngine(*args, **kwargs)
    elif engine == "pandas":
        from optimus.engines.pandas import PandasEngine
        return PandasEngine(*args, **kwargs)
    elif engine == "rapids":
        raise NotImplementedError('rapids')
