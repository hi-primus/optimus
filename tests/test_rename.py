from optimus.tests.base import TestBase


class TestRenamePandas(TestBase):
    dict = {"foobar": [1], "zoobat": [1], "foobat": [1], "john": [1]}

    def test_names_auto_regex(self):

        df = self.df

        result = df.cols.names(".*bat")

        expected = ["zoobat", "foobat"]

        self.assertEquals(result, expected)

    def test_rename_regex(self):

        df = self.df

        result = df.cols.rename(".*bat", func=lambda a: a+"bat").cols.names()

        expected = ["foobar", "zoobatbat", "foobatbat", "john"]

        self.assertEquals(result, expected)

    def test_rename(self):

        df = self.df

        result_single = df.cols.rename("foobar", "FB").cols.names()
        result = df.cols.rename(["foobar", "zoobat"], ["FB", "ZB"]).cols.names()
        result_2 = df.cols.rename([("foobar", "FB"), ("zoobat", "ZB")]).cols.names()
        result_3 = df.cols.rename({"foobar": "FB", "zoobat": "ZB"}).cols.names()

        expected_single = ["FB", "zoobat", "foobat", "john"]
        expected = ["FB", "ZB", "foobat", "john"]

        self.assertEquals(result_single, expected_single)
        self.assertEquals(result, expected)
        self.assertEquals(result_2, expected)
        self.assertEquals(result_3, expected)


class TestRenameDask(TestRenamePandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestRenamePartitionDask(TestRenamePandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestRenameCUDF(TestRenamePandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestRenameDC(TestRenamePandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestRenamePartitionDC(TestRenamePandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}

try:
    import pyspark
except:
    pass
else:
    class TestRenameSpark(TestRenamePandas):
        config = {'engine': 'spark'}

try:
    import vaex
except:
    pass
else:
    class TestRenameVaex(TestRenamePandas):
        config = {'engine': 'vaex'}