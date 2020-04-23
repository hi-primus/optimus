from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.meta import meta
from optimus.outliers.outliers import outliers
from optimus.plots import plots


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


def optimus(engine="dask", *args, **kwargs):
    logger.print("ENGINE", engine)
    # Monkey Patching
    import pandas as pd
    PandasDataFrame = pd.DataFrame

    # from optimus.engines.pandas import rows, columns, extension, constants, functions
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

        from optimus.engines.dask import columns, rows, extension, functions
        from optimus.engines.base.dask import constants
        from optimus.engines.dask.io import save

        DaskDataFrame.outliers = property(outliers)
        DaskDataFrame.meta = property(meta)
        DaskDataFrame.schema = [MetadataDask()]

    if engine == "cudf":
        from cudf.core import DataFrame as CUDFDataFrame

        from optimus.engines.cudf import columns, rows, extension, functions, constants

        from optimus.engines.cudf.io import save

        CUDFDataFrame.outliers = property(outliers)
        CUDFDataFrame.meta = property(meta)
        CUDFDataFrame.schema = [MetadataDask()]

    if engine == "dask-cudf":
        from dask_cudf.core import DataFrame as DaskCUDFDataFrame

        from optimus.engines.dask_cudf import columns, rows, extension, functions
        from optimus.engines.base.dask import constants
        from optimus.engines.dask_cudf.io import save

        DaskCUDFDataFrame.outliers = property(outliers)
        DaskCUDFDataFrame.meta = property(meta)
        DaskCUDFDataFrame.schema = [MetadataDask()]

    if engine == "vaex":
        from vaex import DataFrame as VaexDataFrame
        # import pandas as pd
        from optimus.engines.vaex import rows, columns, extension, constants, functions
        from optimus.engines.vaex.io import save

        VaexDataFrame.outliers = property(outliers)
        VaexDataFrame.meta = property(meta)
        VaexDataFrame.schema = [MetadataDask()]

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

    elif engine == "cudf":
        from optimus.engines.cudf.engine import CUDFEngine
        return CUDFEngine(*args, **kwargs)

    elif engine == "dask_cudf":
        from optimus.engines.dask_cudf.engine import DaskCUDFEngine
        return DaskCUDFEngine(*args, **kwargs)
    else:
        RaiseIt.value_error(engine, ["pandas","spark", "dask", "cudf", "dask_cudf"])
