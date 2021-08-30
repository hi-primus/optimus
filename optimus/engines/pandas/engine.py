import pandas as pd

from optimus.optimus import Engine, EnginePretty
from optimus.engines.base.engine import BaseEngine
from optimus.engines.pandas.create import Create
from optimus.engines.pandas.io.extract import Extract
from optimus.engines.pandas.io.load import Load
from optimus.engines.pandas.pandas import Pandas
from optimus._version import __version__

Pandas.instance = None


class PandasEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False, *args, **kwargs):
        self.extract = Extract()

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
        return Engine.PANDAS.value

    @property
    def engine_label(self):
        return EnginePretty.PANDAS.value

    def remote_run(self, callback, *args, **kwargs):
        if kwargs.get("client_timeout"):
            del kwargs["client_timeout"]

        callback(op=self, *args, **kwargs)

    def remote_submit(self, callback, *args, **kwargs):
        return self.submit(callback, op=self, *args, **kwargs)

    def submit(self, func, *args, priority=0, pure=False, **kwargs):
        import uuid
        def _func():
            return func(*args, **kwargs)

        return type('MockFuture', (object,), {"result": _func, "key": str(uuid.uuid4()), "status": "finished"})
