from optimus.server.functions import run, code, engine, session, create_session, delete_session, features

class Engine:
    
    def __init__(self, config=None, session=None, **kwargs):

        if session is not None:
            self.id = session.id
        else:
            self.id = "default"
        
        if isinstance(config, str):
            engineConfig = {"engine": config}
            config = None
        else:
            engineConfig = {}
        
        engineConfig.update(config or kwargs)
        e = engine(self.id, engineConfig)
        self.name = e["updated"]

    def request(self, body=None, **kwargs):
        body = body or kwargs
        if "source" not in body:
            body.update({"source": self.name})
        return run(self.id, body)