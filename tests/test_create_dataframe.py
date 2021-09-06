from optimus.helpers.functions import results_equal
from optimus.tests.base import TestBase


class TestDFPandas(TestBase):
    def test_dataframe_dict_auto(self):
        data = {'object': [1, "2", 3.0], "int": [1, 2, 3], "float": [1.0, 2.0, 3.5]}
        df = self.op.create.dataframe(data)
        result = df.cols.data_types()
        expected = {'object': 'object', 'int': 'int64', 'float': 'float64'}

        self.assertTrue(results_equal(result, expected))

    def test_dataframe_dict_forced(self):
        data = {('object', 'int64'): [1, "2", 3.0], ("int", 'float'): [1, 2, 3], ("float", 'object'): [1.0, 2.0, 3.5]}
        df = self.op.create.dataframe(data, force_data_types=True)
        result = df.cols.data_types()
        expected = {'object': 'int64', 'int': 'float64', 'float': 'object'}

        self.assertTrue(results_equal(result, expected))

    def test_dataframe_pandas(self):
        import pandas as pd
        data = pd.util.testing.makeMixedDataFrame()
        df = self.op.create.dataframe(data)
        result = df.cols.data_types()
        expected = {'A': 'float64', 'B': 'float64', 'C': 'object', 'D': 'datetime64[ns]'}

        self.assertTrue(results_equal(result, expected))

    def test_dataframe_dict_forced(self):
        data = {('object', 'int64'): [1, "2", 3.0], ("int", 'float'): [1, 2, 3], ("float", 'object'): [1.0, 2.0, 3.5]}
        df = self.op.create.dataframe(data, force_data_types=True)
        result = df.cols.data_types()
        expected = {'object': 'int64', 'int': 'float64', 'float': 'object'}

        self.assertTrue(results_equal(result, expected))


class TestDFDask(TestDFPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestDFPartitionDask(TestDFPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestDFCUDF(TestDFPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestDFDC(TestDFPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestDFPartitionDC(TestDFPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}

try:
    import pyspark
except:
    pass
else:
    class TestDFSpark(TestDFPandas):
        config = {'engine': 'spark'}

try:
    import vaex
except:
    pass
else:
    class TestDFVaex(TestDFPandas):
        config = {'engine': 'vaex'}