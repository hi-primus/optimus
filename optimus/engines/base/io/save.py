class BaseSave:

    def file(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")
    
    def csv(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")
    
    def xml(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")
    
    def json(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def excel(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def avro(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")
    
    def parquet(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def orc(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")

    def hdf5(self, path, *args, **kwargs):
        raise NotImplementedError("Not implemented yet")
