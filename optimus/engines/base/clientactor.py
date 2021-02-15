class ClientActor():
    op = {}
    _vars = {}

    def __init__(self, engine=False):
        if not engine:
            from optimus.optimus import Engine
            engine = Engine.DASK.value

        from optimus import Optimus
        self.op = Optimus(engine)
        self.op.set_var = self.set_var
        self.op.get_var = self.get_var
        self.op.list_vars = self.list_vars
        self.op.update_vars = self.update_vars
        
    def list_vars(self):
        return list(self._vars.keys())

    def update_vars(self, values):
        self._vars.update(values)

    def del_var(self, name):
        try:
            del self._vars[name]
        except:
            print(name + " not found")
        
    def set_var(self, name, value):
        self._vars[name] = value

    def get_var(self, name):
        return self._vars.get(name, None)

    def _primitive(self, value):
        if isinstance(value, (dict,)):
            for key in value:
                value[key] = self._primitive(value[key])
            return value
        elif isinstance(value, (list,)):
            return list(map(self._primitive, value))
        elif isinstance(value, (set,)):
            return set(map(self._primitive, value))
        elif isinstance(value, (tuple,)):
            return tuple(map(self._primitive, value))
        elif not isinstance(value, (str, bool, int, float, complex)):
            return type(value)
        else:
            return value

    def submit(self, callback, *args, **kwargs):
        try:
            result = callback(self.op, *args, **kwargs)
        except Exception as err:
            import traceback
            error_class = err.__class__.__name__
            detail = err.args[0]
            tb = traceback.format_exc()
            error = "%s: %s\n%s" % (error_class, detail, tb)
            return {"status": "error", "error": error}
        return self._primitive(result)