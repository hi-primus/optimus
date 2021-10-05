from optimus.helpers.functions import results_equal
from optimus.tests.base import TestBase


class TestDFPandas(TestBase):
    def test_dataframe_dict_auto(self):
        data = {'object': [1, "2", 3.0], "int": [1, 2, 3], "float": [1.0, 2.0, 3.5]}
        df = self.op.create.dataframe(data)
        result = df.cols.data_type()
        expected = {'object': 'object', 'int': 'int64', 'float': 'float64'}

        self.assertTrue(results_equal(result, expected))

    def test_dataframe_dict_forced(self):
        data = {('object', 'int64'): [1, "2", 3.0], ("int", 'float'): [1, 2, 3], ("float", 'object'): [1.0, 2.0, 3.5]}
        df = self.op.create.dataframe(data, force_data_types=True)
        result = df.cols.data_type()
        expected = {'object': 'int64', 'int': 'float64', 'float': 'object'}

        self.assertTrue(results_equal(result, expected))

    def test_dataframe_pandas(self):
        import pandas as pd
        data = pd.util.testing.makeMixedDataFrame()
        df = self.op.create.dataframe(data)
        result = df.cols.data_type()
        expected = {'A': 'float64', 'B': 'float64', 'C': 'object', 'D': 'datetime64[ns]'}

        self.assertTrue(results_equal(result, expected))

    def test_dataframe_dict_forced(self):
        data = {('object', 'int64'): [1, "2", 3.0], ("int", 'float'): [1, 2, 3], ("float", 'object'): [1.0, 2.0, 3.5]}
        df = self.op.create.dataframe(data, force_data_types=True)
        result = df.cols.data_type()
        expected = {'object': 'int64', 'int': 'float64', 'float': 'object'}

        self.assertTrue(results_equal(result, expected))


class TestDFDask(TestDFPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestDFPartitionDask(TestDFPandas):
    config = {'engine': 'dask', 'n_partitions': 2}

