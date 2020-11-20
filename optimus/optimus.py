from enum import Enum

from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt

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
    IBIS = "ibis"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


def optimus(engine=Engine.DASK.value, *args, **kwargs):
    logger.print("ENGINE", engine)

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

    elif engine == Engine.IBIS.value:
        from optimus.engines.ibis.engine import IbisEngine
        return IbisEngine(*args, **kwargs)

    else:
        RaiseIt.value_error(engine, Engine.list())
