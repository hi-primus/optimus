import unittest

class TestBase(unittest.TestCase):

    config = { "engine": "pandas", "n_partitions": 1 }
    dict = {}
    load = {}

    @classmethod
    def setUpClass(cls):
        if not cls.config:
            raise Exception("Please initialize device before running tests")
        if not cls.dict and not cls.load:
            raise Exception("No dataframe was defined for this test")

    def setUp(self):
        import sys
        sys.path.append("..")
        from optimus import Optimus
        self.op = Optimus(self.config["engine"])

        if self.dict:
            self.df = self.create_dataframe(self.dict)
        elif self.load:
            if isinstance(self.load, str):
                self.df = self.load_dataframe(self.load)
            else:
                self.df = self.load_dataframe(**self.load)
        self.post_create()

    def create_dataframe(self, dict=None, **kwargs):        
        if not "n_partitions" in kwargs and "n_partitions" in self.config:
            kwargs["n_partitions"] = self.config["n_partitions"]

        return self.op.create.dataframe(dict, **kwargs)

    def load_dataframe(self, path=None, type='file', **kwargs):        
        if not "n_partitions" in kwargs and "n_partitions" in self.config:
            kwargs["n_partitions"] = self.config["n_partitions"]

        return getattr(self.op.load, type)(path, **kwargs)

    def post_create(self):
        pass

