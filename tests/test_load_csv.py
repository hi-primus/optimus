from optimus.tests.base import TestBase


class TestCSVPandas(TestBase):

    def test_csv_txt(self):
        df = self.load_dataframe("examples/data/DCIGNP2AYL.txt", n_rows=50)
        self.assertEqual(df.rows.count(), 50)
        # TO-DO: assert columns are properly loaded
        # TO-DO: assert content is properly loaded (can be a sample)

class TestCSVDask(TestCSVPandas):
    config = {"engine": "dask", "n_partitions": 1}

class TestCSVPartitionDask(TestCSVPandas):
    config = {"engine": "dask", "n_partitions": 2}