import pandas as pd

from optimus.engines.base.engine import BaseEngine
from optimus.engines.pandas.create import Create
from optimus.engines.pandas.io.extract import Extract
from optimus.engines.pandas.io.load import Load
from optimus.engines.pandas.pandas import Pandas
from optimus.version import __version__

Pandas.instance = None


class PandasEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False, comm=None, *args, **kwargs):
        self.extract = Extract()

        self.verbose(verbose)

        Pandas.instance = pd

        self.client = pd

    @property
    def create(self):
        return Create(self)

    @property
    def load(self):
        return Load(self)

    @property
    def engine(self):
        return "pandas"

