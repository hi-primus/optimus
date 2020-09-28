from enum import Enum

from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt

from optimus.plots import plots

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
    # columns, rows, constants, extension, functions, save, plots

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