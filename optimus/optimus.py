from optimus.helpers.logger import logger
from optimus.meta import meta
from optimus.outliers.outliers import outliers


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


from optimus.plots import plots


def optimus(engine="spark", *args, **kwargs):
    logger.print("ENGINE", engine)
    # Monkey Patching

    if engine == "vaex":
        from vaex import DataFrame as VaexDataFrame
        # import pandas as pd
        from optimus.engines.vaex import rows, columns, extension, constants, functions
        from optimus.engines.vaex.io import save

        VaexDataFrame.outliers = property(outliers)
        VaexDataFrame.meta = property(meta)
        VaexDataFrame.schema = [MetadataDask()]

    if engine == "pandas":
        from pandas import DataFrame as PandasDataFrame
        # import pandas as pd
        from optimus.engines.pandas import rows, columns, extension, constants, functions
        from optimus.engines.pandas.io import save


        PandasDataFrame.outliers = property(outliers)
        PandasDataFrame.meta = property(meta)
        PandasDataFrame.schema = [MetadataDask()]

    if engine == "spark":
        from pyspark.sql import DataFrame as SparkDataFrame

        # pyspark_pipes: build Spark ML pipelines easily
        from optimus.engines.spark.ml.pipelines import patch
        patch()

        from optimus.engines.spark import rows, columns, extension, constants, functions
        from optimus.engines.spark.io import save


        SparkDataFrame.outliers = property(outliers)
        SparkDataFrame.meta = property(meta)

    if engine == "dask" or engine == "dask-cudf":
        from dask.dataframe.core import DataFrame as DaskDataFrame

        from optimus.engines.dask import columns, rows, constants, extension, functions
        from optimus.engines.dask.io import save

        DaskDataFrame.outliers = property(outliers)
        DaskDataFrame.meta = property(meta)
        DaskDataFrame.schema = [MetadataDask()]

    if engine == "dask-cudf":
        from dask_cudf.core import DataFrame as DaskCUDFDataFrame

        from optimus.engines.dask_cudf import columns, rows, constants, extension, functions
        from optimus.engines.dask_cudf.io import save

        DaskCUDFDataFrame.outliers = property(outliers)
        DaskCUDFDataFrame.meta = property(meta)
        DaskCUDFDataFrame.schema = [MetadataDask()]

    # Dummy so pycharm not complain about not used imports
    columns, rows, constants, extension, functions, save, plots
    # else:
    #     RaiseIt.value_error(engine, ["spark", "cudf", "dask-cudf"])

    # Init engine
    if engine == "pandas":
        from optimus.engines.pandas.engine import PandasEngine
        return PandasEngine(*args, **kwargs)

    elif engine == "spark":
        from optimus.engines.spark.engine import SparkEngine
        return SparkEngine(*args, **kwargs)

    elif engine == "dask":
        from optimus.engines.dask.engine import DaskEngine
        return DaskEngine(*args, **kwargs)

    elif engine == "dask-cudf":
        from optimus.engines.dask_cudf.engine import DaskCUDFEngine
        return DaskCUDFEngine(*args, **kwargs)
