from optimus.tests.base import TestBase
import datetime
Timestamp = lambda t: datetime.datetime.strptime(t,"%Y-%m-%d %H:%M:%S")
nan = float("nan")
inf = float("inf")
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort, df_dicts_equal

class TestExamplePandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('name', 'object'): ['Optimus', 'Bumblebee', 'Eject'], ('age (M)', 'int64'): [5, 5, 5]}
    maxDiff = None
    
    def test_cols_upper_multiple(self):
        df = self.df
        result = df.cols.upper(cols=['name', 'age (M)'])
        result = result.to_dict()
        expected = {'name': ['OPTIMUS', 'BUMBLEBEE', 'EJECT'], 'age (M)': ['5', '5', '5']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_upper_single(self):
        df = self.df
        result = df.cols.upper(cols=['name'])
        expected = self.create_dataframe(dict={('name', 'string'): ['OPTIMUS', 'BUMBLEBEE', 'EJECT'], ('age (M)', 'int64'): [5, 5, 5]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True))

class TestExampleDask(TestExamplePandas):
    config = {'engine': 'dask', 'n_partitions': 1}

class TestExampleDask2(TestExamplePandas):
    config = {'engine': 'dask', 'n_partitions': 2}
