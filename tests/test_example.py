import unittest
from test_base import TestBase

class TestCols(TestBase):
    dict = {"A": [-1, 2, 3, -10, -1.5, 50]}
    load = {} # {"type": "csv", "path": "foo.csv"} | {"path": "foo.csv"} | "foo.csv"
    def test_rows(self):
        self.assertGreater(self.df.rows.count(), -1)
    
    def test_name(self):
        self.assertGreater(len(str(self.df.cols.names()[0])), 0)

    def test_abs(self):
        # self.create_dataframe uses the config of the test
        # expected_df = self.create_dataframe({"A": [1, 2, 3, 10, 1.5, 50]})
        
        # can compare an Optimus dataframe and a Python dictionary
        expected_df = {"A": [1, 2, 3, 10, 1.5, 50]}

        self.assertTrue( self.df.cols.abs().equals(expected_df) )

class TestColsDask(TestCols):
    config = { "engine": "dask", "n_partitions": 1 }

class TestColsDask2(TestCols):
    config = { "engine": "dask", "n_partitions": 2 }

class TestColsDask3(TestCols):
    config = { "engine": "dask", "n_partitions": 3 }

