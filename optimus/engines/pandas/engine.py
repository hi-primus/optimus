import numpy as np
import pandas as pd

from optimus.bumblebee import Comm
from optimus.engines.base.engine import BaseEngine, op_to_series_func
from optimus.engines.pandas.io.extract import Extract
from optimus.engines.pandas.io.load import Load
from optimus.engines.pandas.pandas import Pandas
from optimus.profiler.profiler import Profiler
from optimus.version import __version__

Pandas.instance = None
Profiler.instance = None
Comm.instance = None


class PandasEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False, comm=None, *args, **kwargs):
        if comm is True:
            Comm.instance = Comm()
        else:
            Comm.instance = comm

        self.engine = 'pandas'
        # self.create = Create()
        self.load = Load()
        self.extract = Extract()

        # self.read = self.spark.read
        self.verbose(verbose)

        Pandas.instance = pd

        self.client = Pandas.instance

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance

