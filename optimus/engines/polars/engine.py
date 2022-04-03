import polars as pl

from optimus._version import __version__
from optimus.engines.base.engine import BaseEngine
from optimus.engines.polars.create import Create
from optimus.engines.polars.io.extract import Extract
from optimus.engines.polars.io.load import Load
from optimus.engines.polars.polars import Polars
from optimus.optimus import Engine, EnginePretty
import uuid

Polars.instance = None


class PolarsEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False, *args, **kwargs):
        self.extract = Extract()

        self.verbose(verbose)

        Polars.instance = pl

        self.client = pl

    @property
    def constants(self):
        from optimus.engines.polars.constants import Constants
        return Constants()

    @property
    def F(self):
        from optimus.engines.polars.functions import PolarsFunctions
        return PolarsFunctions()

    @property
    def create(self):
        return Create(self)

    @property
    def load(self):
        return Load(self)

    @property
    def engine(self):
        return Engine.POLARS.value

    @property
    def engine_label(self):
        return EnginePretty.POLARS.value

    def remote_run(self, callback, *args, **kwargs):
        if kwargs.get("client_timeout"):
            del kwargs["client_timeout"]

        callback(op=self, *args, **kwargs)

    def remote_submit(self, callback, *args, **kwargs):
        return self.submit(callback, op=self, *args, **kwargs)

    def submit(self, func, *args, priority=0, pure=False, **kwargs):

        def _func():
            return func(*args, **kwargs)

        return type('MockFuture', (object,), {"result": _func, "key": str(uuid.uuid4()), "status": "finished"})
