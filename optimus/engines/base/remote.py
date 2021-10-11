from pandas import DataFrame as PandasDataFrame

from optimus.engines.base.basedataframe import BaseDataFrame

MAX_TIMEOUT = 600


class RemoteDummyAttribute:

    def __init__(self, name, names, dummy_id, op):
        self.__names = [*names, name]
        self.__op = op
        self.__id = dummy_id

    def __getattr__(self, item):
        return RemoteDummyAttribute(item, self.__names, self.__id, self.__op)

    def __call__(self, *args, **kwargs):

        if kwargs.get("client_submit"):
            client_submit = kwargs["client_submit"]
            del kwargs["client_submit"]
        else:
            client_submit = False

        def _f(op, unique_id, method, *args, **kwargs):
            obj = op.get_var(unique_id)
            if obj is None:
                op.del_var(unique_id)
                raise Exception("Remote variable with id " + unique_id + " not found or null")
            func = obj
            for me in method:
                func = getattr(func, me)
            if callable(func):
                result = func(*args, **kwargs)
            else:
                result = func
            return result

        if client_submit:
            return self.__op.remote_submit(_f, self.__id, self.__names, *args, **kwargs)
        else:
            return self.__op.remote_run(_f, self.__id, self.__names, *args, **kwargs)


class RemoteDummyVariable:

    def __init__(self, op, unique_id, *args, **kwargs):
        self.op = op
        self.id = unique_id

    def __getattr__(self, item):
        if item.startswith('_'):
            raise AttributeError(item)
        return RemoteDummyAttribute(item, [], self.id, self.op)

    def __getstate__(self):
        return {"op": self.op, "id": self.id}

    def __setstate__(self, d):
        self.op = d.op
        self.id = d.id
        return

    def __del__(self):
        self.op.remote.del_var(self.id).result(180)


class RemoteDummyDataFrame(RemoteDummyVariable):
    print = BaseDataFrame.print
    table = BaseDataFrame.table
    display = BaseDataFrame.display

    def __repr__(self):
        return self.ascii()

    def _repr_html_(self):
        return self.table()

    def __len__(self):
        return self.rows.count()

    @property
    def meta(self):
        def _get_attr(op, unique_id, attr):
            df = op.get_var(unique_id)
            if df is None:
                op.del_var(unique_id)
                raise Exception("Remote variable with id " + unique_id + " not found or null")
            return getattr(df, attr)

        return self.op.remote_run(_get_attr, self.id, "meta")


class RemoteOptimusInterface:
    op = {}
    _vars = {}
    _del_next = []

    def __init__(self, client, engine=False):
        if not engine:
            from optimus.optimus import Engine
            engine = Engine.DASK.value

        self.client = client
        self.engine = engine

        future = self._create_actor(self.engine)
        future.result(MAX_TIMEOUT)

    def _create_actor(self, engine):
        self.engine = engine

        def _init(_engine):
            from distributed import get_worker
            worker = get_worker()
            worker.actor = RemoteOptimus(_engine)
            return f"Created remote Optimus instance using \"{_engine}\""

        return self.client.submit(_init, self.engine)

    def submit(self, func, *args, priority=0, pure=False, **kwargs):
        def _remote(_func, *args, **kwargs):
            from distributed import get_worker
            actor = get_worker().actor
            return actor.submit(_func, priority=priority, pure=pure, *args, **kwargs)

        kwargs.update({"pure": False})

        return self.client.submit(_remote, func, *args, **kwargs)

    def run(self, func, *args, **kwargs):
        if kwargs.get("client_timeout"):
            client_timeout = kwargs.get("client_timeout")
            del kwargs["client_timeout"]
        else:
            client_timeout = MAX_TIMEOUT

        return self.submit(func, *args, **kwargs).result(client_timeout)

    def list_vars(self, client_timeout=MAX_TIMEOUT):
        def _list_vars():
            from distributed import get_worker
            op = get_worker().actor.op
            return op.list_vars()

        return self.client.submit(_list_vars, pure=False).result(client_timeout)

    def clear_vars(self, keep=[], client_timeout=MAX_TIMEOUT):
        def _clear_vars(keep=[]):
            from distributed import get_worker
            op = get_worker().actor.op
            return op.clear_vars(keep)

        return self.client.submit(_clear_vars, keep, pure=False).result(client_timeout)

    def update_vars(self, values, client_timeout=MAX_TIMEOUT):
        def _update_vars(values):
            from distributed import get_worker
            op = get_worker().actor.op
            return op.update_vars(values)

        return self.client.submit(_update_vars, values, pure=False).result(client_timeout)

    def del_var(self, name, client_timeout=MAX_TIMEOUT):
        def _del_var(name):
            from distributed import get_worker
            op = get_worker().actor.op
            return op.del_var(name)

        return self.client.submit(_del_var, name, pure=False).result(client_timeout)

    def set_var(self, name, value, client_timeout=MAX_TIMEOUT):
        def _set_var(name):
            from distributed import get_worker
            op = get_worker().actor.op
            return op.set_var(name, value)

        return self.client.submit(_set_var, name, value, pure=False).result(client_timeout)

    def get_var(self, name, client_timeout=MAX_TIMEOUT):
        def _get_var(name):
            from distributed import get_worker
            op = get_worker().actor.op
            return op.get_var(name)

        return self.client.submit(_get_var, name, pure=False).result(client_timeout)


class RemoteOptimus:
    op = {}
    _vars = {}
    _del_next = []

    def __init__(self, engine=False):
        from optimus.optimus import Engine

        if not engine:
            engine = Engine.DASK.value

        from optimus import Optimus
        self.op = Optimus(engine)
        self.op.set_var = self.set_var
        self.op.get_var = self.get_var
        self.op.del_var = self.del_var
        self.op.clear_vars = self.clear_vars
        self.op.list_vars = self.list_vars
        self.op.update_vars = self.update_vars
        self.set_var("_load", self.op.load)
        self.set_var("_create", self.op.create)

        import numpy as np
        if engine == Engine.DASK_CUDF.value:
            import cupy as cp
            self.allowed_types = (str, bool, int, float, complex, np.generic, cp.generic)
        else:
            self.allowed_types = (str, bool, int, float, complex, np.generic)

    def list_vars(self):
        return list(self._vars.keys())

    def clear_vars(self, keep=[]):
        keep = keep + ["_load", "_create"]
        self._vars = {k: self._vars[k] for k in keep}
        return list(self._vars.keys())

    def update_vars(self, values):
        self._vars.update(values)

    def _del_var(self, name):
        try:
            del self._vars[name]
        except:
            print(name + " not found")

    def del_var(self, name):

        for _name in self._del_next:
            self._del_var(_name)

        self._del_next = []

        if not name.startswith("_"):
            if name not in self._vars or self._vars[name] is None:
                print(name + " not found")
            else:
                self._del_next.append(name)

    def set_var(self, name, value):
        self._vars[name] = value

    def get_var(self, name):
        return self._vars.get(name, None)

    def _return(self, value):
        if isinstance(value, (dict,)):
            for key in value:
                value[key] = self._return(value[key])
            return value
        elif isinstance(value, (list,)):
            return list(map(self._return, value))
        elif isinstance(value, (set,)):
            return set(map(self._return, value))
        elif isinstance(value, (tuple,)):
            return tuple(map(self._return, value))
        elif isinstance(value, (PandasDataFrame,)):
            return value.head()
        elif not isinstance(value, self.allowed_types) and value is not None:
            import uuid
            unique_id = str(uuid.uuid4())
            self.set_var(unique_id, value)
            if isinstance(value, (BaseDataFrame,)):
                return {"dummy": unique_id, "dataframe": True}
            else:
                return {"dummy": unique_id, "dataframe": False}
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
        return self._return(result) if result is not None else None
