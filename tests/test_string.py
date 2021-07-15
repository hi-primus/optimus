from optimus.tests.base import TestBase
import datetime
nan = float("nan")
inf = float("inf")
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort

class TestStringPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('name', 'string'): ['Optimus', 'Bumblebee', 'Eject'], ('age (M)', 'int32'): [5, 5, 5]}
    maxDiff = None
    
    def test_cols_upper_multiple(self):
        result = self.df.cols.upper(cols=['name', 'age (M)'])
        result = result.to_dict()
        expected = {'name': ['OPTIMUS', 'BUMBLEBEE', 'EJECT'], 'age (M)': ['5', '5', '5']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_upper_single(self):
        result = self.df.cols.upper(cols=['name'])
        result = result.to_dict()
        expected = {'name': ['OPTIMUS', 'BUMBLEBEE', 'EJECT'], 'age (M)': [5, 5, 5]}
        self.assertDictEqual(deep_sort(result), deep_sort(expected))

class TestStringDask(TestBase):
    config = {'engine': 'dask', 'n_partitions': 1}

class TestStringDask2(TestBase):
    config = {'engine': 'dask', 'n_partitions': 2}
