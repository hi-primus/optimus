from optimus.tests.base import TestBase


class TestExamplePandas(TestBase):
    dict = {"A": [-1, 2, 3, -10, -1.5, 50]}
    load = {}  # {"type": "csv", "path": "foo.csv"} | {"path": "foo.csv"} | "foo.csv"

    def test_rows(self):
        self.assertGreater(self.df.rows.count(), -1)

    def test_name(self):
        self.assertGreater(len(str(self.df.cols.names()[0])), 0)

    def test_abs(self):
        # self.create_dataframe uses the config of the test
        # expected_df = self.create_dataframe({"A": [1, 2, 3, 10, 1.5, 50]})

        # can compare an Optimus dataframe and a Python dictionary
        expected_df = {"A": [1, 2, 3, 10, 1.5, 50]}

        self.assertTrue(self.df.cols.abs().equals(expected_df))


class TestExampleDask(TestExamplePandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestExamplePartitionDask(TestExamplePandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestExampleCUDF(TestExamplePandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestExampleDC(TestExamplePandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestExamplePartitionDC(TestExamplePandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark
except:
    pass
else:
    class TestExampleSpark(TestExamplePandas):
        config = {'engine': 'spark'}

try:
    import vaex
except:
    pass
else:
    class TestExampleVaex(TestExamplePandas):
        config = {'engine': 'vaex'}
