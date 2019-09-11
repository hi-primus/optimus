from optimus.engines.pandas import PandasEngine
from optimus.engines.spark import SparkEngine


class Optimus:
    @staticmethod
    def start(engine, *args, **kwargs):
        if engine == "spark":
            return SparkEngine(*args, **kwargs)
        elif engine == "pandas":
            return PandasEngine(*args, **kwargs)
        elif engine == "rapids":
            print("not implemented yet")
