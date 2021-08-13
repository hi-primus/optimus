from enum import Enum

from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
import nltk


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

    # Init engine
    if engine == Engine.PANDAS.value:
        from optimus.engines.pandas.engine import PandasEngine
        op = PandasEngine(*args, **kwargs)

    elif engine == Engine.VAEX.value:
        from optimus.engines.vaex.engine import VaexEngine
        op = VaexEngine(*args, **kwargs)

    elif engine == Engine.SPARK.value:
        from optimus.engines.spark.engine import SparkEngine
        op = SparkEngine(*args, **kwargs)

    elif engine == Engine.DASK.value:
        from optimus.engines.dask.engine import DaskEngine
        op = DaskEngine(*args, **kwargs)

    elif engine == Engine.IBIS.value:
        from optimus.engines.ibis.engine import IbisEngine
        op = IbisEngine(*args, **kwargs)

    elif engine == Engine.CUDF.value:
        from optimus.engines.cudf.engine import CUDFEngine
        op = CUDFEngine(*args, **kwargs)

    elif engine == Engine.DASK_CUDF.value:
        from optimus.engines.dask_cudf.engine import DaskCUDFEngine
        op = DaskCUDFEngine(*args, **kwargs)

    else:
        RaiseIt.value_error(engine, Engine.list())

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
