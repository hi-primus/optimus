import cudf
from optimus.engines.base.engine import BaseEngine
from optimus.engines.cudf.create import Create
from optimus.engines.cudf.cudf import CUDF
from optimus.engines.cudf.io.load import Load
from optimus.version import __version__

CUDF.instance = None


class CUDFEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False, *args, **kwargs):
        self.verbose(verbose)

        CUDF.instance = cudf

        self.client = CUDF.instance

    @property
    def create(self):
        return Create(self)

    @property
    def load(self):
        return Load(self)

    @property
    def engine(self):
        return "cudf"

    def remote_run(self, callback, *args, **kwargs):
        return {"result": callback(*args, **kwargs)}

    def remote_submit(self, callback, *args, **kwargs):
        return {"result": callback(*args, **kwargs)}

    def submit(self, func, *args, **kwargs):
        return {"result": func(*args, **kwargs)}
