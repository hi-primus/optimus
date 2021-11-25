from optimus.tests.base import TestBase


class TestLoadPandas(TestBase):

    def test_json(self):
        df = self.load_dataframe("examples/data/foo.json", type="json", multiline=True)
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_json_less_rows(self):
        df = self.load_dataframe("examples/data/foo.json", type="json", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_json_more_rows(self):
        df = self.load_dataframe("examples/data/foo.json", type="json", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_xml(self):
        df = self.load_dataframe("examples/data/foo.xml", type="xml")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_xml_less_rows(self):
        df = self.load_dataframe("examples/data/foo.xml", type="xml", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_xml_more_rows(self):
        df = self.load_dataframe("examples/data/foo.xml", type="xml", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_parquet(self):
        df = self.load_dataframe("examples/data/foo.parquet", type="parquet")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_parquet_less_rows(self):
        df = self.load_dataframe("examples/data/foo.parquet", type="parquet", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_parquet_more_rows(self):
        df = self.load_dataframe("examples/data/foo.parquet", type="parquet", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_avro(self):
        df = self.load_dataframe("examples/data/foo.avro", type="avro")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_avro_less_rows(self):
        df = self.load_dataframe("examples/data/foo.avro", type="avro", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_avro_more_rows(self):
        df = self.load_dataframe("examples/data/foo.avro", type="avro", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_tsv(self):
        df = self.load_dataframe("examples/data/foo.tsv", type="tsv")
        self.assertEqual(df.rows.count(), 5)
        self.assertEqual(df.cols.names(), ["Sepal length", "Sepal width", "Petal length", "Petal width", "Species"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_tsv_less_rows(self):
        df = self.load_dataframe("examples/data/foo.tsv", type="tsv", n_rows=3)
        self.assertEqual(df.rows.count(), 3)
        self.assertEqual(df.cols.names(), ["Sepal length", "Sepal width", "Petal length", "Petal width", "Species"])        
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_tsv_more_rows(self):
        df = self.load_dataframe("examples/data/foo.tsv", type="tsv", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["Sepal length", "Sepal width", "Petal length", "Petal width", "Species"])        
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_xls(self):
        df = self.load_dataframe("examples/data/titanic3.xls", type="excel")
        self.assertEqual(df.rows.count(), 1309)
        self.assertEqual(df.cols.names(), ["pclass", "survived", "name", "sex", "age", "sibsp", "parch", "ticket", "fare", "cabin", "embarked", "boat", "body", "home.dest"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_xls_less_rows(self):
        df = self.load_dataframe("examples/data/titanic3.xls", type="excel", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["pclass", "survived", "name", "sex", "age", "sibsp", "parch", "ticket", "fare", "cabin", "embarked", "boat", "body", "home.dest"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

    def test_xls_more_rows(self):
        df = self.load_dataframe("examples/data/titanic3.xls", type="excel", n_rows=50)
        self.assertLess(df.rows.count(), 5000)
        self.assertEqual(df.cols.names(), ["pclass", "survived", "name", "sex", "age", "sibsp", "parch", "ticket", "fare", "cabin", "embarked", "boat", "body", "home.dest"])
        if "n_partitions" in self.config:
            self.assertEqual(self.config["n_partitions"], df.partitions())

class TestLoadDask(TestLoadPandas):
    config = {'engine': 'dask'}


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
        config = {'engine': 'dask_cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestLoadPartitionDC(TestLoadPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


try:
    import pyspark
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