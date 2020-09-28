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


# Monkey Patching
# import pandas as pd

import importlib

if importlib.util.find_spec("pandas") is not None:
    print("pandas")
    import pandas as pd
    PandasDataFrame = pd.DataFrame

    from optimus.engines.pandas import rows, columns, extension, constants, functions
    from optimus.engines.pandas.io import save

    PandasDataFrame.outliers = property(outliers)
    PandasDataFrame.meta = property(meta)
    PandasDataFrame.schema = [MetadataDask()]

if importlib.util.find_spec("dask") is not None:
    # We are using dask for all the database operations for cudf, das_cudf and dask
    from dask.dataframe.core import DataFrame as DaskDataFrame

    from optimus.engines.dask import columns, rows, extension, functions
    from optimus.engines.base.dask import constants
    from optimus.engines.dask.io import save

    DaskDataFrame.outliers = property(outliers)
    DaskDataFrame.meta = property(meta)
    DaskDataFrame.schema = [MetadataDask()]

if importlib.util.find_spec("spark") is not None:
    from pyspark.sql import DataFrame as SparkDataFrame

    # pyspark_pipes: build Spark ML pipelines easily
    from optimus.engines.spark.ml.pipelines import patch

    patch()

    from optimus.engines.spark import rows, columns, extension, constants, functions
    from optimus.engines.spark.io import save

    SparkDataFrame.outliers = property(outliers)
    SparkDataFrame.meta = property(meta)

if importlib.util.find_spec("cudf") is not None:
    import cudf
    from cudf.core import DataFrame as CUDFDataFrame

    from optimus.engines.cudf import columns, rows, extension, functions, constants

    from optimus.engines.cudf.io import save

    CUDFDataFrame.outliers = property(outliers)
    CUDFDataFrame.meta = property(meta)
    CUDFDataFrame.schema = [MetadataDask()]
    CUDFDataFrame._lib = cudf

if importlib.util.find_spec("dask_cudf") is not None:
    from dask_cudf.core import DataFrame as DaskCUDFDataFrame
    from optimus.engines.dask_cudf import columns, rows, extension, functions
    from optimus.engines.base.dask import constants
    from optimus.engines.dask_cudf.io import save

    DaskCUDFDataFrame.outliers = property(outliers)
    DaskCUDFDataFrame.meta = property(meta)
    DaskCUDFDataFrame.schema = [MetadataDask()]

# if importlib.util.find_spec("vaex") is not None:
#     from vaex import DataFrame as VaexDataFrame
#     # import pandas as pd
#     from optimus.engines.vaex import rows, columns, extension, constants, functions
#     from optimus.engines.vaex.io import save
#
#     VaexDataFrame.outliers = property(outliers)
#     VaexDataFrame.meta = property(meta)
#     VaexDataFrame.schema = [MetadataDask()]