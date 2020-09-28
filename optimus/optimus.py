from enum import Enum

from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.meta import meta
from optimus.outliers.outliers import outliers
from optimus.plots import plots

# Monkey Patching
# import pandas as pd


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


class Engine(Enum):
    PANDAS = "pandas"
    CUDF = "cudf"
    DASK = "dask"
    DASK_CUDF = "dask_cudf"
    SPARK = "spark"
    VAEX = "vaex"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


def optimus(engine=Engine.DASK.value, *args, **kwargs):
    logger.print("ENGINE", engine)
    # # Monkey Patching
    # import pandas as pd
    # PandasDataFrame = pd.DataFrame
    # PandasDataFrame._lib = pd
    #
    # # from optimus.engines.pandas import rows, columns, extension, constants, functions
    # from optimus.engines.pandas import rows, columns, extension, constants, functions
    # from optimus.engines.pandas.io import save
    #
    # PandasDataFrame.outliers = property(outliers)
    # PandasDataFrame.meta = property(meta)
    # PandasDataFrame.schema = [MetadataDask()]
    #
    # # if engine == Engine.DASK.value or engine == Engine.DASK_CUDF.value:
    # # We are using dask for all the database operations for cudf, das_cudf and dask
    # from dask.dataframe.core import DataFrame as DaskDataFrame
    #
    # from optimus.engines.dask import columns, rows, extension, functions
    # from optimus.engines.base.dask import constants
    # from optimus.engines.dask.io import save
    #
    # DaskDataFrame.outliers = property(outliers)
    # DaskDataFrame.meta = property(meta)
    # DaskDataFrame.schema = [MetadataDask()]

    if engine == Engine.CUDF.value or engine == Engine.DASK_CUDF.value:
        import rmm
        import cupy
        # Switch to RMM allocator
        cupy.cuda.set_allocator(rmm.rmm_cupy_allocator)


    # Dummy so pycharm not complain about not used imports
    columns, rows, constants, extension, functions, save, plots

    # Init engine
    if engine == Engine.PANDAS.value:
        from optimus.engines.pandas.engine import PandasEngine
        return PandasEngine(*args, **kwargs)

    elif engine == Engine.SPARK.value:
        from optimus.engines.spark.engine import SparkEngine
        return SparkEngine(*args, **kwargs)

    elif engine == Engine.DASK.value:
        from optimus.engines.dask.engine import DaskEngine
        return DaskEngine(*args, **kwargs)

    elif engine == Engine.CUDF.value:
        from optimus.engines.cudf.engine import CUDFEngine
        return CUDFEngine(*args, **kwargs)

    elif engine == Engine.DASK_CUDF.value:
        from optimus.engines.dask_cudf.engine import DaskCUDFEngine
        return DaskCUDFEngine(*args, **kwargs)
    else:
        RaiseIt.value_error(engine, Engine.list())