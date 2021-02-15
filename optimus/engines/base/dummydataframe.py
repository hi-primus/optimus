class AttributeChain():
    
    __names = []
    __df = None
    
    def __init__(self, name ,names = [], df = None):
        self.__names = [*names, name]
        self.__df = df
    
    def __getattr__(self, item):
        return AttributeChain(item, self.__names, self.__df)
    
    def __call__(self, *args, **kwargs):
        return self.__df.call_on_client(self.__names, *args, **kwargs)
    

class DummyDataFrame:

    op = None
    # id = None

    def __init__(self, op, unique_id, *args, **kwargs):
        self.op = op
        self.id = unique_id
    
    def __getattr__(self, item):
        return AttributeChain(item, [], self)
    
    def __del__(self):

        def _f(op, unique_id, *args, **kwargs):
            op.del_var(unique_id)
        
        self.op.client_run(_f, self.id)
    
    def call_on_client(self, method, *args, **kwargs):

        def _f(op, unique_id, method, *args, **kwargs):
            df = op.get_var(unique_id)
            func = df
            for me in method:
                func = getattr(func, me)
            result = func(*args, **kwargs)
            if result.__class__ == df.__class__:
                import uuid
                unique_id_2 = str(uuid.uuid4()) # get unique id
                op.set_var(unique_id_2, result)
                return DummyDataFrame(op, unique_id_2)
            else:
                return self._primitive(result)
                
        return self.op.client_run(_f, self.id, method, *args, **kwargs)
        
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

