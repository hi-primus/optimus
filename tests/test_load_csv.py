from optimus.tests.base import TestBase


class TestCSVPandas(TestBase):

    def test_csv(self):
        df = self.load_dataframe("examples/data/foo.csv")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])

    def test_csv_13rows(self):
        df = self.load_dataframe("examples/data/foo.csv", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_csv_50rows(self):
        df = self.load_dataframe("examples/data/foo.csv", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

class TestCSVDask(TestCSVPandas):
    config = {"engine": "dask", "n_partitions": 1}

class TestCSVPartitionDask(TestCSVPandas):
    config = {"engine": "dask", "n_partitions": 2}