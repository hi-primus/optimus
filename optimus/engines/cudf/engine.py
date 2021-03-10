from optimus.engines.base.engine import BaseEngine

from optimus.engines.cudf.cudf import CUDF
from optimus.engines.cudf.io.load import Load
from optimus.version import __version__

CUDF.instance = None


class CUDFEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False, *args, **kwargs):
        import cudf
        self.verbose(verbose)

        CUDF.instance = cudf

        self.client = CUDF.instance

    @property
    def create(self):
        from optimus.engines.cudf.create import Create
        return Create(self)

    @property
    def load(self):
        return Load(self)

    @property
    def engine(self):
        return "cudf"

    def remote_run(self, callback, *args, **kwargs):
        if kwargs.get("client_timeout"):
            del kwargs["client_timeout"]

        callback(*args, **kwargs)

    def remote_submit(self, callback, *args, **kwargs):
        return self.submit(callback, op=self, *args, **kwargs)

    def submit(self, func, *args, **kwargs):
        import uuid
        def _func():
            return func(*args, **kwargs)
        return {"result": _func, "key": str(uuid.uuid4()), "status": "finished"}