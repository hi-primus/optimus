import pandas as pd

from optimus._version import __version__
from optimus.engines.base.engine import BaseEngine
from optimus.engines.pandas.create import Create
from optimus.engines.pandas.pandas import Pandas
# from optimus.engines.pandas.io.extract import Extract
from optimus.engines.pyscript.io.load import Load
from optimus.optimus import Engine, EnginePretty

Pandas.instance = None


class PyScriptEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False, *args, **kwargs):
        # self.extract = Extract()
        self.verbose(verbose)
        Pandas.instance = pd
        self.client = pd

    @property
    def constants(self):
        from optimus.engines.pandas.constants import Constants
        return Constants()

    @property
    def F(self):
        from optimus.engines.pandas.functions import PandasFunctions
        return PandasFunctions()

    @property
    def create(self):
        return Create(self)

    @property
    def load(self):
        return Load(self)

    @property
    def engine(self):
        return Engine.PYSCRIPT.value

    @property
    def engine_label(self):
        return EnginePretty.PANDAS.value
