import datetime
import numpy as np
from optimus.tests.base import TestBase
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort, df_dicts_equal, results_equal


def Timestamp(t):
    return datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")


NaT = np.datetime64('NaT')
nan = float("nan")
inf = float("inf")


class TestExamplePandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('name', 'object'): ['Optimus', 'Bumblebee', 'Eject'], ('age (M)', 'int64'): [5, 5, 5]}
    maxDiff = None

    def test_cols_upper_multiple(self):
        df = self.df.copy()
        result = df.cols.upper(cols=['name', 'age (M)'])
        result = result.to_dict()
        expected = {'name': ['OPTIMUS', 'BUMBLEBEE', 'EJECT'], 'age (M)': ['5', '5', '5']}
        self.assertEqual(json_encoding(result), json_encoding(expected))

    def test_cols_upper_single(self):
        df = self.df.copy()
        result = df.cols.upper(cols=['name'])
        expected = self.create_dataframe(data={('name', 'object'): ['OPTIMUS', 'BUMBLEBEE', 'EJECT'], ('age (M)', 'int64'): [5, 5, 5]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))


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
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestExampleSpark(TestExamplePandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestExampleVaex(TestExamplePandas):
        config = {'engine': 'vaex'}
