from optimus.engines.pandas import PandasEngine
from optimus.engines.spark import SparkEngine


def optimus(engine, *args, **kwargs):
    if engine == "spark":
        return SparkEngine(*args, **kwargs)
    elif engine == "pandas":
        print("not implemented yet")
    elif engine == "rapids":
        print("not implemented yet")