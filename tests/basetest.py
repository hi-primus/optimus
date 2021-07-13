import unittest

class TestBase(unittest.TestCase):

    def __init__(self, testName, engine="pandas", n_partitions=1):
        super(TestBase, self).__init__(testName)
        
        import sys
        sys.path.append("..")
        from optimus import Optimus

        self.engine = engine
        self.n_partitions = n_partitions

        self.op = Optimus(self.engine)
        self.df = self.get_dataframe(self.n_partitions)

    def get_dataframe(self, n_partitions):
        return self.op.create.dataframe({('Numbers', 'int'): [0, "1", 2.0, 3.1]})
    
    def test_dataframe(self):
        self.assertEqual(self.df.rows.count(), 4)
    
    def test_name(self):
        self.assertEqual(self.df.cols.names()[0], 'Numbers')

def tests(cls, engines):
    tests = []
    for engine in engines:
        tests.extend([ cls(attr, engine) for attr in dir(TestBase) if attr.startswith("test_") ])
    return tests

def run_tests(clss, engines=['pandas', 'dask']):
    suite = unittest.TestSuite()
    for cls in clss:
        suite.addTests(tests(cls, engines))
    runner = unittest.TextTestRunner()
    runner.run(suite)


if __name__ == '__main__':
    run_tests([TestBase])