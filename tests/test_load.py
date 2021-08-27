from optimus.tests.base import TestBase


class TestLoadPandas(TestBase):

    def test_json(self):
        df = self.load_dataframe("examples/data/foo.json", type="json")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])

    def test_json_13rows(self):
        df = self.load_dataframe("examples/data/foo.json", type="json", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_json_50rows(self):
        df = self.load_dataframe("examples/data/foo.json", type="json", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_xml(self):
        df = self.load_dataframe("examples/data/foo.xml", type="xml")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])

    def test_xml_13rows(self):
        df = self.load_dataframe("examples/data/foo.xml", type="xml", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_xml_50rows(self):
        df = self.load_dataframe("examples/data/foo.xml", type="xml", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_parquet(self):
        df = self.load_dataframe("examples/data/foo.parquet", type="parquet")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])

    def test_parquet_13rows(self):
        df = self.load_dataframe("examples/data/foo.parquet", type="parquet", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_parquet_50rows(self):
        df = self.load_dataframe("examples/data/foo.parquet", type="parquet", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_avro(self):
        df = self.load_dataframe("examples/data/foo.avro", type="avro")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])

    def test_avro_13rows(self):
        df = self.load_dataframe("examples/data/foo.avro", type="avro", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_avro_50rows(self):
        df = self.load_dataframe("examples/data/foo.avro", type="avro", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_tsv(self):
        df = self.load_dataframe("examples/data/foo.tsv", type="tsv")
        self.assertEqual(df.rows.count(), 5)
        self.assertEqual(df.cols.names(), ["Sepal length",	"Sepal width",	"Petal length",	"Petal width",	"Species"])

    def test_tsv_3rows(self):
        df = self.load_dataframe("examples/data/foo.tsv", type="tsv", n_rows=3)
        self.assertEqual(df.rows.count(), 3)
        self.assertEqual(df.cols.names(), ["Sepal length",	"Sepal width",	"Petal length",	"Petal width",	"Species"])        

    def test_tsv_50rows(self):
        df = self.load_dataframe("examples/data/foo.tsv", type="tsv", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["Sepal length",	"Sepal width",	"Petal length",	"Petal width",	"Species"])        

    def test_xls(self):
        df = self.load_dataframe("examples/data/titanic3.xls", type="xls")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])

    def test_xls_13rows(self):
        df = self.load_dataframe("examples/data/titanic3.xls", type="xls", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

    def test_xls_50rows(self):
        df = self.load_dataframe("examples/data/titanic3.xls", type="xls", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id","firstName","lastName","billingId","product","price","birth","dummyCol"])        

class TestLoadDask(TestLoadPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestLoadPartitionDask(TestLoadPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestLoadCUDF(TestLoadPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestLoadDC(TestLoadPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestLoadPartitionDC(TestLoadPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import spark
except:
    pass
else:
    class TestLoadSpark(TestLoadPandas):
        config = {'engine': 'spark'}

try:
    import vaex
except:
    pass
else:
    class TestLoadVaex(TestLoadPandas):
        config = {'engine': 'vaex'}