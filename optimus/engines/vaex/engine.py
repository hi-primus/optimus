from optimus.engines.base.engine import BaseEngine
from optimus.engines.vaex.create import Create
from optimus.engines.vaex.io.load import Load
from optimus.optimus import Engine
from optimus.version import __version__
import vaex


class VaexEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False):
        self.verbose(verbose)
        self.client = vaex

    @property
    def create(self):
        return Create(self)

    @property
    def load(self):
        return Load(self)

    @property
    def engine(self):
        return Engine.VAEX.value
