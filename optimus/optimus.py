from enum import Enum

import nltk

from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt


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


class EnginePretty(Enum):
    PANDAS = "Pandas"
    CUDF = "cuDF"
    DASK = "Dask"
    DASK_CUDF = "Dask-cuDF"
    SPARK = "Spark"
    VAEX = "Vaex"
    IBIS = "Ibis"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


def engine_function(engine, func, *args, **kwargs):
    if engine in Engine.list():
        return func[engine](*args, **kwargs)
    else:
        RaiseIt.value_error(engine, Engine.list())


def start_pandas(*args, **kwargs):
    from optimus.engines.pandas.engine import PandasEngine
    return PandasEngine(*args, **kwargs)


def start_vaex(*args, **kwargs):
    from optimus.engines.vaex.engine import VaexEngine
    return VaexEngine(*args, **kwargs)


def start_spark(*args, **kwargs):
    import databricks.koalas as ks
    """
        Koalas disallows the operations on different DataFrames (or Series) by default to prevent expensive operations.
        It internally performs a join operation which can be expensive in general.
    """
    ks.set_option('compute.ops_on_diff_frames', True)
    from optimus.engines.spark.engine import SparkEngine
    return SparkEngine(*args, **kwargs)


def start_dask(*args, **kwargs):
    from optimus.engines.dask.engine import DaskEngine
    return DaskEngine(*args, **kwargs)


def start_ibis(*args, **kwargs):
    from optimus.engines.ibis.engine import IbisEngine
    return IbisEngine(*args, **kwargs)


def start_cudf(*args, **kwargs):
    from optimus.engines.cudf.engine import CUDFEngine
    return CUDFEngine(*args, **kwargs)


def start_dask_cudf(*args, **kwargs):
    from optimus.engines.dask_cudf.engine import DaskCUDFEngine
    return DaskCUDFEngine(*args, **kwargs)


def optimus(engine=Engine.DASK.value, *args, **kwargs):
    """
    This is the entry point to initialize the selected engine.
    :param engine: A string identifying an engine :classL`Engine`.
    :param args:
    :param kwargs:
    :return:
    """
    logger.print("ENGINE", engine)

    # lemmatizer
    nltk.download('wordnet', quiet=True)

    # Stopwords
    nltk.download('stopwords', quiet=True)

    # POS
    nltk.download('averaged_perceptron_tagger', quiet=True)

    funcs = {Engine.PANDAS.value: start_pandas,
             Engine.VAEX.value: start_vaex,
             Engine.SPARK.value: start_spark,
             Engine.DASK.value: start_dask,
             Engine.IBIS.value: start_ibis,
             Engine.CUDF.value: start_cudf,
             Engine.DASK_CUDF.value: start_dask_cudf
             }

    op = engine_function(engine, funcs)

    # Set cupy yo user RMM
    def switch_to_rmm_allocator():
        import rmm
        import cupy
        cupy.cuda.set_allocator(rmm.rmm_cupy_allocator)
        return True

    if engine == Engine.CUDF.value:
        switch_to_rmm_allocator()

    if engine == Engine.DASK_CUDF.value:
        if op.client:
            op.client.run(switch_to_rmm_allocator)

    return op
