from optimus.tests.base import TestBase


class TestCSVPandas(TestBase):

    def test_csv(self):
        df = self.load_dataframe("../../examples/data/foo.csv")
        self.assertEqual(df.rows.count(), 19)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])

    def test_csv_less_rows(self):
        df = self.load_dataframe("../../examples/data/foo.csv", n_rows=13)
        self.assertEqual(df.rows.count(), 13)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])

    def test_csv_more_rows(self):
        df = self.load_dataframe("../../examples/data/foo.csv", n_rows=50)
        self.assertLess(df.rows.count(), 50)
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])
    
    def test_csv_no_header(self):
        df = self.load_dataframe("../../examples/data/foo.csv", header=None)
        self.assertEqual(df.cols.names(), [0,1,2,3,4,5,6,7])
    
    def test_csv_null(self):
        df = self.load_dataframe("../../examples/data/foo.csv", null_value="null")
        self.assertEqual(df.mask.null(cols='product').cols.frequency(), {'frequency': {'product': {'values': [{'value': False, 'count': 18},
                        {'value': True, 'count': 1}]}}})
        
    def test_csv_semicolon(self):
        df = self.load_dataframe("../../exmaples/data/foo.csv", sep=";")
        self.assertEqual(df.cols.names(), ['id,firstName,lastName,billingId,product,price,birth,dummyCol'])
        
    def test_csv_coma(self):
        df = self.load_dataframe("../../exmaples/data/foo.csv", sep=",")
        self.assertEqual(df.cols.names(), ["id", "firstName", "lastName", "billingId", "product", "price", "birth", "dummyCol"])

class TestCSVDask(TestCSVPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestCSVPartitionDask(TestCSVPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestCSVCUDF(TestCSVPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestCSVDC(TestCSVPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestCSVPartitionDC(TestCSVPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}

try:
    import pyspark
except:
    pass
else:
    class TestCSVSpark(TestCSVPandas):
        config = {'engine': 'spark'}

try:
    import vaex
except:
    pass
else:
    class TestCSVVaex(TestCSVPandas):
        config = {'engine': 'vaex'}