from pandas import DataFrame as PandasDataFrame
from optimus.engines.base.basedataframe import BaseDataFrame

class RemoteDummyAttribute:
    
    def __init__(self, name, names, dummy_id, op):
        self.__names = [*names, name]
        self.__op = op
        self.__id = dummy_id
        
    
    def __getattr__(self, item):
        return RemoteDummyAttribute(item, self.__names, self.__id, self.__op)
    
    
    def __call__(self, *args, **kwargs):

        def _f(op, unique_id, method, *args, **kwargs):
            obj = op.get_var(unique_id)
            if obj is None:
                op.del_var(unique_id)
                raise Exception("Remote variable with id "+unique_id+" not found or null")
            func = obj
            for me in method:
                func = getattr(func, me)
            result = func(*args, **kwargs)
            return result

        return self.__op.remote_run(_f, self.__id, self.__names, *args, **kwargs)


class RemoteDummyVariable:

    def __init__(self, op, unique_id, *args, **kwargs):
        self.op = op
        self.id = unique_id
    
    def __getattr__(self, item):
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

    __repr__ = BaseDataFrame.__repr__
    _repr_html_ = BaseDataFrame._repr_html_
    print = BaseDataFrame.print
    display = BaseDataFrame.display
    table = BaseDataFrame.table
        
    
class ClientActor:
    op = {}
    _vars = {}
    _del_next = []

    def __init__(self, engine=False):
        if not engine:
            from optimus.optimus import Engine
            engine = Engine.DASK.value

        from optimus import Optimus
        self.op = Optimus(engine)
        self.op.set_var = self.set_var
        self.op.get_var = self.get_var
        self.op.del_var = self.del_var
        self.op.list_vars = self.list_vars
        self.op.update_vars = self.update_vars
        self.set_var("_load", self.op.load)
        self.set_var("_create", self.op.create)
        
    def list_vars(self):
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
        
        if self._vars[name] is None:
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
        elif not isinstance(value, (str, bool, int, float, complex)) and value is not None:
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