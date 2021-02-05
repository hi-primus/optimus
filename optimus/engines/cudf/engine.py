import cudf

from optimus.engines.cudf.create import Create
from optimus.engines.base.engine import BaseEngine
from optimus.engines.cudf.cudf import CUDF
from optimus.engines.cudf.dataframe import CUDFDataFrame
from optimus.engines.cudf.io.load import Load
from optimus.profiler.profiler import Profiler
from optimus.version import __version__

CUDF.instance = None
Profiler.instance = None


class CUDFEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False, *args, **kwargs):
        self.verbose(verbose)

        CUDF.instance = cudf

        self.client = CUDF.instance

        # Profiler.instance = Profiler()
        # self.profiler = Profiler.instance

    @property
    def create(self):
        return Create(self)

    @property
    def load(self):
        return Load(self)

    @property
    def engine(self):
        return "cudf"

    # def dataframe(self, pdf, *args, **kwargs):
    #     from dask import dataframe as dd
    #     return CUDFDataFrame(pdf)
