from optimus.tests.base import TestBase


class TestUDFPandas(TestBase):
    load = {"path": "examples/data/DCIGNP2AYL.txt", "n_rows": 20}

    def test_udf(self):
        from optimus.functions import F

        df = self.df

        def haversine(lat1, lon1, lat2, lon2):    
            MILES = 3959
            lat1, lon1, lat2, lon2 = map(F.radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1 
            dlon = lon2 - lon1
            a = F.sin(dlat / 2)**2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2)**2
            c = 2 * F.asin(F.sqrt(a))  
            total_miles = MILES * c
            return total_miles

        df["distance"] = haversine(40.671, -73.985, df['latitude'], df['longitude'])

        self.assertIn("distance", df.cols.names())

        expected_dict = {"distance": [139.6071899, 139.7468976, 142.1910502, 137.275546, 139.6845834, 142.511427,
                                      139.2236552, 137.5044141, 141.9774911, 138.9195547, 141.1156307, 137.320386,
                                      136.9785681, 139.8973388, 142.0501961, 141.0701359, 141.6853331, 141.7333734,
                                      139.0531985, 139.5583856]}

        self.assertTrue(df["distance"].equals(expected_dict, decimal=True, assertion=True))


class TestUDFDask(TestUDFPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestUDFPartitionDask(TestUDFPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestUDFCUDF(TestUDFPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestUDFDC(TestUDFPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestUDFPartitionDC(TestUDFPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}

try:
    import pyspark
except:
    pass
else:
    class TestUDFSpark(TestUDFPandas):
        config = {'engine': 'spark'}

try:
    import vaex
except:
    pass
else:
    class TestUDFVaex(TestUDFPandas):
        config = {'engine': 'vaex'}