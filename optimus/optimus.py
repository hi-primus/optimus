from optimus.outliers.outliers import outliers
from optimus.meta import meta
from optimus.plots import plots
from optimus.helpers.raiseit import RaiseIt

# This class emulate how spark metadata handling works.
class MetadataDask:
    def __init__(self):
        self._metadata = {}

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

def optimus(engine="spark", *args, **kwargs):
    if engine == "spark":

        from pyspark.sql import DataFrame as SparkDataFrame

        # Monkey patch
        # pyspark_pipes: build Spark ML pipelines easily
        from optimus.engines.spark.ml.pipelines import patch
        patch()

        from optimus.engines.spark.engine import SparkEngine

        from optimus.engines.spark import rows, columns, extension, constants, functions
        from optimus.engines.spark.io import save

        SparkDataFrame.outliers = property(outliers)
        SparkDataFrame.meta = property(meta)

        return SparkEngine(*args, **kwargs)

    elif engine == "dask":
        from dask.dataframe.core import DataFrame as DaskDataFrame

        # Monkey patch

        from optimus.engines.dask.engine import DaskEngine
        from optimus.engines.dask import columns, rows, constants, extension, functions
        from optimus.engines.dask.io import save

        DaskDataFrame.outliers = property(outliers)
        DaskDataFrame.meta = property(meta)



        DaskDataFrame.schema = [MetadataDask()]

        return DaskEngine(*args, **kwargs)
    elif engine == "dask-cudf":
        from dask.dataframe.core import DataFrame as DaskDataFrame

        # Monkey Patch
        from optimus.engines.dask_cudf.engine import DaskCUDFEngine
        # from optimus.engines.dask_cudf import columns, rows, constants, extension, functions
        # from optimus.engines.dask_cudf.io import save
        DaskDataFrame.outliers = property(outliers)
        DaskDataFrame.meta = property(meta)
        DaskDataFrame.schema = [MetadataDask()]

        return DaskCUDFEngine(*args, **kwargs)
    else:
        RaiseIt.value_error(engine, ["spark", "cudf", "dask-cudf"])
    # elif engine == "pandas":
    #     from optimus.engines.pandas import PandasEngine
    #     return PandasEngine(*args, **kwargs)
