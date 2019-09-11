from optimus.engines.spark import SparkEngine


class Optimus:
    @staticmethod
    def start(engine, *args, **kwargs):
        if engine == "spark":
            return SparkEngine(*args, **kwargs)
        elif engine == "pandas":
            print("not implemented yet")
        elif engine == "rapids":
            print("not implemented yet")
