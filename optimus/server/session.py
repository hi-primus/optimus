import uuid

from optimus.server.functions import run, code, engine, session, create_session, delete_session, features

from .engine import Engine

defaultEngineConfiguration = { "engine": "dask" }

class Session:
    
    def __init__(self, config=None, id=None, **kwargs):
        self.config = config or kwargs
        self.id = id or str(uuid.uuid4())
        self._default_engine = None
        if "engineConfiguration" not in self.config:
            self.config.update({"engineConfiguration": defaultEngineConfiguration})

    def engine(self, config=None, **kwargs):
        config = config or kwargs or self.config["engineConfiguration"]
        e = Engine(config, session=self)
        if self._default_engine is None:
            self._default_engine = e

        return e

    @property
    def default_engine(self):
        if self._default_engine is None:
            return self.engine()

        return self._default_engine

    def request(self, body=None, **kwargs):
        return self.default_engine.request(body, **kwargs)
        
