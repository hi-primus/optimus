from optimus.tests.base import TestBase
import datetime
Timestamp = lambda t: datetime.datetime.strptime(t,"%Y-%m-%d %H:%M:%S")
nan = float("nan")
inf = float("inf")
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort, df_dicts_equal

class TestMoreStatisticsPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('id', 'int64'): [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], ('name', 'object'): ['pants', 'shoes', 'shirt', 'pants', 'pants', 'shoes', 'pants', 'pants', 'shirt', 'pants'], ('code', 'object'): ['L15', 'SH', 'RG30', 'J10', 'JG15', 'B', 'JG20', 'L20', 'FT50', 'JG15'], ('price', 'float64'): [173.47, 69.99, 30.0, 34.99, 132.99, 57.99, 179.99, 95.0, 50.0, 169.99], ('discount', 'object'): ['0', '15%', '5%', '0', '0', '20%', '15%', '0', '0', '0']}
    maxDiff = None
    
    def test_cols_boxplot_all(self):
        df = self.df
        result = df.cols.boxplot(cols='*')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_boxplot_multiple(self):
        df = self.df
        result = df.cols.boxplot(cols=['id', 'code', 'discount'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_boxplot_numeric(self):
        df = self.df
        result = df.cols.boxplot(cols='price')
        expected = {'price': {'mean': 99.441, 'median': 82.495, 'q1': 51.9975, 'q3': 160.74, 'whisker_low': -111.11625000000001, 'whisker_high': 323.85375, 'fliers': [], 'label': 'price'}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_correlation_all_kendall(self):
        df = self.df
        result = df.cols.correlation('*','kendall')
        expected = {'id': {'id': 1.0, 'price': 0.1111111111111111}, 'price': {'id': 0.1111111111111111, 'price': 1.0}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_correlation_all_pearson(self):
        df = self.df
        result = df.cols.correlation('*','pearson')
        expected = {'id': {'id': 1.0, 'price': 0.15785706335886504}, 'price': {'id': 0.15785706335886504, 'price': 1.0}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_correlation_all_spearman(self):
        df = self.df
        result = df.cols.correlation('*','spearman')
        expected = {'id': {'id': 1.0, 'price': 0.1393939393939394}, 'price': {'id': 0.1393939393939394, 'price': 1.0}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_correlation_multiple_kendall(self):
        df = self.df
        result = df.cols.correlation(['id', 'price'],'kendall')
        expected = 0.1111111111111111
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_correlation_multiple_pearson(self):
        df = self.df
        result = df.cols.correlation(['id', 'price'],'pearson')
        expected = 0.15785706335886504
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_correlation_multiple_spearman(self):
        df = self.df
        result = df.cols.correlation(['id', 'price'],'spearman')
        expected = 0.1393939393939394
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_correlation_numeric_kendall(self):
        df = self.df
        result = df.cols.correlation('price','kendall')
        expected = {'price': {'price': 1.0}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_correlation_numeric_pearson(self):
        df = self.df
        result = df.cols.correlation('price','pearson')
        expected = {'price': {'price': 1.0}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_correlation_numeric_spearman(self):
        df = self.df
        result = df.cols.correlation('price','spearman')
        expected = {'price': {'price': 1.0}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_count_uniques_all(self):
        df = self.df
        result = df.cols.count_uniques(cols='*')
        expected = {'id': 10, 'name': 3, 'code': 9, 'price': 10, 'discount': 4}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_count_uniques_multiple(self):
        df = self.df
        result = df.cols.count_uniques(cols=['id', 'code', 'discount'],estimate=False)
        expected = {'id': 10, 'code': 9, 'discount': 4}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_count_uniques_numeric(self):
        df = self.df
        result = df.cols.count_uniques(cols='price',estimate=True)
        expected = 10
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_count_zeros_all(self):
        df = self.df
        result = df.cols.count_zeros(cols='*')
        expected = {'id': 0, 'name': 0, 'code': 0, 'price': 0, 'discount': 0}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_count_zeros_multiple(self):
        df = self.df
        result = df.cols.count_zeros(cols=['id', 'code', 'discount'])
        expected = {'id': 0, 'code': 0, 'discount': 0}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_count_zeros_numeric(self):
        df = self.df
        result = df.cols.count_zeros(cols='price')
        expected = 0
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_crosstab_numeric_numeric(self):
        df = self.df
        result = df.cols.crosstab(col_x='discount',col_y='price',output='dict')
        expected = {30.0: {'0': 0, '15%': 0, '20%': 0, '5%': 1}, 34.99: {'0': 1, '15%': 0, '20%': 0, '5%': 0}, 50.0: {'0': 1, '15%': 0, '20%': 0, '5%': 0}, 57.99: {'0': 0, '15%': 0, '20%': 1, '5%': 0}, 69.99: {'0': 0, '15%': 1, '20%': 0, '5%': 0}, 95.0: {'0': 1, '15%': 0, '20%': 0, '5%': 0}, 132.99: {'0': 1, '15%': 0, '20%': 0, '5%': 0}, 169.99: {'0': 1, '15%': 0, '20%': 0, '5%': 0}, 173.47: {'0': 1, '15%': 0, '20%': 0, '5%': 0}, 179.99: {'0': 0, '15%': 1, '20%': 0, '5%': 0}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_crosstab_numeric_string(self):
        df = self.df
        result = df.cols.crosstab(col_x='price',col_y='code',output='dataframe')
        expected = self.create_dataframe(dict={('price', 'float64'): [30.0, 34.99, 50.0, 57.99, 69.99, 95.0, 132.99, 169.99, 173.47, 179.99], ('B', 'int64'): [0, 0, 0, 1, 0, 0, 0, 0, 0, 0], ('FT50', 'int64'): [0, 0, 1, 0, 0, 0, 0, 0, 0, 0], ('J10', 'int64'): [0, 1, 0, 0, 0, 0, 0, 0, 0, 0], ('JG15', 'int64'): [0, 0, 0, 0, 0, 0, 1, 1, 0, 0], ('JG20', 'int64'): [0, 0, 0, 0, 0, 0, 0, 0, 0, 1], ('L15', 'int64'): [0, 0, 0, 0, 0, 0, 0, 0, 1, 0], ('L20', 'int64'): [0, 0, 0, 0, 0, 1, 0, 0, 0, 0], ('RG30', 'int64'): [1, 0, 0, 0, 0, 0, 0, 0, 0, 0], ('SH', 'int64'): [0, 0, 0, 0, 1, 0, 0, 0, 0, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_crosstab_string_numeric(self):
        df = self.df
        result = df.cols.crosstab(col_x='name',col_y='id',output='dataframe')
        expected = self.create_dataframe(dict={('name', 'object'): ['pants', 'shirt', 'shoes'], ('1', 'int64'): [1, 0, 0], ('2', 'int64'): [0, 0, 1], ('3', 'int64'): [0, 1, 0], ('4', 'int64'): [1, 0, 0], ('5', 'int64'): [1, 0, 0], ('6', 'int64'): [0, 0, 1], ('7', 'int64'): [1, 0, 0], ('8', 'int64'): [1, 0, 0], ('9', 'int64'): [0, 1, 0], ('10', 'int64'): [1, 0, 0]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_crosstab_string_string(self):
        df = self.df
        result = df.cols.crosstab(col_x='code',col_y='name',output='dict')
        expected = {'pants': {'B': 0, 'FT50': 0, 'J10': 1, 'JG15': 2, 'JG20': 1, 'L15': 1, 'L20': 1, 'RG30': 0, 'SH': 0}, 'shirt': {'B': 0, 'FT50': 1, 'J10': 0, 'JG15': 0, 'JG20': 0, 'L15': 0, 'L20': 0, 'RG30': 1, 'SH': 0}, 'shoes': {'B': 1, 'FT50': 0, 'J10': 0, 'JG15': 0, 'JG20': 0, 'L15': 0, 'L20': 0, 'RG30': 0, 'SH': 1}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_frequency_all(self):
        df = self.df
        result = df.cols.frequency(cols='*',n=10,count_uniques=True)
        expected = {'frequency': {'id': {'values': [{'value': 1, 'count': 1}, {'value': 2, 'count': 1}, {'value': 3, 'count': 1}, {'value': 4, 'count': 1}, {'value': 5, 'count': 1}, {'value': 6, 'count': 1}, {'value': 7, 'count': 1}, {'value': 8, 'count': 1}, {'value': 9, 'count': 1}, {'value': 10, 'count': 1}], 'count_uniques': 10}, 'name': {'values': [{'value': 'pants', 'count': 6}, {'value': 'shoes', 'count': 2}, {'value': 'shirt', 'count': 2}], 'count_uniques': 3}, 'code': {'values': [{'value': 'JG15', 'count': 2}, {'value': 'L15', 'count': 1}, {'value': 'SH', 'count': 1}, {'value': 'RG30', 'count': 1}, {'value': 'J10', 'count': 1}, {'value': 'B', 'count': 1}, {'value': 'JG20', 'count': 1}, {'value': 'L20', 'count': 1}, {'value': 'FT50', 'count': 1}], 'count_uniques': 9}, 'price': {'values': [{'value': 173.47, 'count': 1}, {'value': 69.99, 'count': 1}, {'value': 30.0, 'count': 1}, {'value': 34.99, 'count': 1}, {'value': 132.99, 'count': 1}, {'value': 57.99, 'count': 1}, {'value': 179.99, 'count': 1}, {'value': 95.0, 'count': 1}, {'value': 50.0, 'count': 1}, {'value': 169.99, 'count': 1}], 'count_uniques': 10}, 'discount': {'values': [{'value': '0', 'count': 6}, {'value': '15%', 'count': 2}, {'value': '5%', 'count': 1}, {'value': '20%', 'count': 1}], 'count_uniques': 4}}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_frequency_multiple(self):
        df = self.df
        result = df.cols.frequency(cols=['id', 'code', 'discount'],n=6,percentage=True)
        expected = {'frequency': {'id': {'values': [{'value': 1, 'count': 1, 'percentage': 10.0}, {'value': 2, 'count': 1, 'percentage': 10.0}, {'value': 3, 'count': 1, 'percentage': 10.0}, {'value': 4, 'count': 1, 'percentage': 10.0}, {'value': 5, 'count': 1, 'percentage': 10.0}, {'value': 6, 'count': 1, 'percentage': 10.0}]}, 'code': {'values': [{'value': 'JG15', 'count': 2, 'percentage': 20.0}, {'value': 'L15', 'count': 1, 'percentage': 10.0}, {'value': 'SH', 'count': 1, 'percentage': 10.0}, {'value': 'RG30', 'count': 1, 'percentage': 10.0}, {'value': 'J10', 'count': 1, 'percentage': 10.0}, {'value': 'B', 'count': 1, 'percentage': 10.0}]}, 'discount': {'values': [{'value': '0', 'count': 6, 'percentage': 60.0}, {'value': '15%', 'count': 2, 'percentage': 20.0}, {'value': '5%', 'count': 1, 'percentage': 10.0}, {'value': '20%', 'count': 1, 'percentage': 10.0}]}}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_frequency_numeric(self):
        df = self.df
        result = df.cols.frequency(cols='price',n=4,percentage=True,total_rows=3)
        expected = {'frequency': {'price': {'values': [{'value': 173.47, 'count': 1, 'percentage': 10.0}, {'value': 69.99, 'count': 1, 'percentage': 10.0}, {'value': 30.0, 'count': 1, 'percentage': 10.0}, {'value': 34.99, 'count': 1, 'percentage': 10.0}]}}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_frequency_string(self):
        df = self.df
        result = df.cols.frequency(cols='name',n=5,percentage=False)
        expected = {'frequency': {'name': {'values': [{'value': 'pants', 'count': 6}, {'value': 'shoes', 'count': 2}, {'value': 'shirt', 'count': 2}]}}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_heatmap_numeric_numeric(self):
        df = self.df
        result = df.cols.heatmap(col_x='discount',col_y='price',bins_x=5,bins_y=10)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_heatmap_numeric_string(self):
        df = self.df
        result = df.cols.heatmap(col_x='price',col_y='code',bins_x=3,bins_y=1)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_heatmap_string_numeric(self):
        df = self.df
        result = df.cols.heatmap(col_x='name',col_y='id',bins_x=7,bins_y=0)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_heatmap_string_string(self):
        df = self.df
        result = df.cols.heatmap(col_x='code',col_y='name',bins_x=4,bins_y=4)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_hist_all(self):
        df = self.df
        result = df.cols.hist(cols='*',buckets=2)
        expected = {'hist': {'id': [{'lower': 1.0, 'upper': 10.0, 'count': 10}], 'name': [{'lower': nan, 'upper': nan, 'count': 10}], 'code': [{'lower': nan, 'upper': nan, 'count': 10}], 'price': [{'lower': 30.0, 'upper': 179.99, 'count': 10}], 'discount': [{'lower': 0.0, 'upper': 0.0, 'count': 6}]}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_hist_multiple(self):
        df = self.df
        result = df.cols.hist(cols=['id', 'code', 'discount'],buckets=4)
        expected = {'hist': {'id': [{'lower': 1.0, 'upper': 4.0, 'count': 3}, {'lower': 4.0, 'upper': 7.0, 'count': 3}, {'lower': 7.0, 'upper': 10.0, 'count': 4}], 'code': [{'lower': nan, 'upper': nan, 'count': 0}, {'lower': nan, 'upper': nan, 'count': 0}, {'lower': nan, 'upper': nan, 'count': 10}], 'discount': [{'lower': 0.0, 'upper': 0.0, 'count': 0}, {'lower': 0.0, 'upper': 0.0, 'count': 0}, {'lower': 0.0, 'upper': 0.0, 'count': 6}]}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_hist_numeric(self):
        df = self.df
        result = df.cols.hist(cols='price',buckets=10)
        expected = {'hist': {'price': [{'lower': 30.0, 'upper': 46.66555555555556, 'count': 2}, {'lower': 46.66555555555556, 'upper': 63.33111111111111, 'count': 2}, {'lower': 63.33111111111111, 'upper': 79.99666666666667, 'count': 1}, {'lower': 79.99666666666667, 'upper': 96.66222222222223, 'count': 1}, {'lower': 96.66222222222223, 'upper': 113.32777777777778, 'count': 0}, {'lower': 113.32777777777778, 'upper': 129.99333333333334, 'count': 0}, {'lower': 129.99333333333334, 'upper': 146.6588888888889, 'count': 1}, {'lower': 146.6588888888889, 'upper': 163.32444444444445, 'count': 0}, {'lower': 163.32444444444445, 'upper': 179.99, 'count': 3}]}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_infer_types_all(self):
        df = self.df
        result = df.cols.infer_types(cols='*')
        expected = {'id': {'data_type': 'int', 'categorical': True}, 'name': {'data_type': 'str', 'categorical': True}, 'code': {'data_type': 'str', 'categorical': True}, 'price': {'data_type': 'decimal', 'categorical': False}, 'discount': {'data_type': 'int', 'categorical': True}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_infer_types_multiple(self):
        df = self.df
        result = df.cols.infer_types(cols=['id', 'code', 'discount'],output_cols=['id_types', 'code_types', 'just a col'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_infer_types_numeric(self):
        df = self.df
        result = df.cols.infer_types(cols='price',output_cols='price_types')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_profile_all(self):
        df = self.df
        result = df.cols.profile(cols='*')
        expected = [{'stats': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'int', 'categorical': True}, 'frequency': [{'value': 1, 'count': 1}, {'value': 2, 'count': 1}, {'value': 3, 'count': 1}, {'value': 4, 'count': 1}, {'value': 5, 'count': 1}, {'value': 6, 'count': 1}, {'value': 7, 'count': 1}, {'value': 8, 'count': 1}, {'value': 9, 'count': 1}, {'value': 10, 'count': 1}], 'count_uniques': 10}, 'data_type': 'int64'}, {'stats': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'str', 'categorical': True}, 'frequency': [{'value': 'pants', 'count': 6}, {'value': 'shoes', 'count': 2}, {'value': 'shirt', 'count': 2}], 'count_uniques': 3}, 'data_type': 'object'}, {'stats': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'str', 'categorical': True}, 'frequency': [{'value': 'JG15', 'count': 2}, {'value': 'L15', 'count': 1}, {'value': 'SH', 'count': 1}, {'value': 'RG30', 'count': 1}, {'value': 'J10', 'count': 1}, {'value': 'B', 'count': 1}, {'value': 'JG20', 'count': 1}, {'value': 'L20', 'count': 1}, {'value': 'FT50', 'count': 1}], 'count_uniques': 9}, 'data_type': 'object'}, {'stats': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'decimal', 'categorical': False}, 'hist': [{'lower': 30.0, 'upper': 34.6871875, 'count': 1}, {'lower': 34.6871875, 'upper': 39.374375, 'count': 1}, {'lower': 39.374375, 'upper': 44.0615625, 'count': 0}, {'lower': 44.0615625, 'upper': 48.74875, 'count': 0}, {'lower': 48.74875, 'upper': 53.4359375, 'count': 1}, {'lower': 53.4359375, 'upper': 58.123125, 'count': 1}, {'lower': 58.123125, 'upper': 62.8103125, 'count': 0}, {'lower': 62.8103125, 'upper': 67.4975, 'count': 0}, {'lower': 67.4975, 'upper': 72.1846875, 'count': 1}, {'lower': 72.1846875, 'upper': 76.871875, 'count': 0}, {'lower': 76.871875, 'upper': 81.55906250000001, 'count': 0}, {'lower': 81.55906250000001, 'upper': 86.24625, 'count': 0}, {'lower': 86.24625, 'upper': 90.9334375, 'count': 0}, {'lower': 90.9334375, 'upper': 95.620625, 'count': 1}, {'lower': 95.620625, 'upper': 100.30781250000001, 'count': 0}, {'lower': 100.30781250000001, 'upper': 104.995, 'count': 0}, {'lower': 104.995, 'upper': 109.6821875, 'count': 0}, {'lower': 109.6821875, 'upper': 114.369375, 'count': 0}, {'lower': 114.369375, 'upper': 119.05656250000001, 'count': 0}, {'lower': 119.05656250000001, 'upper': 123.74375, 'count': 0}, {'lower': 123.74375, 'upper': 128.4309375, 'count': 0}, {'lower': 128.4309375, 'upper': 133.11812500000002, 'count': 1}, {'lower': 133.11812500000002, 'upper': 137.8053125, 'count': 0}, {'lower': 137.8053125, 'upper': 142.4925, 'count': 0}, {'lower': 142.4925, 'upper': 147.1796875, 'count': 0}, {'lower': 147.1796875, 'upper': 151.866875, 'count': 0}, {'lower': 151.866875, 'upper': 156.55406250000001, 'count': 0}, {'lower': 156.55406250000001, 'upper': 161.24125, 'count': 0}, {'lower': 161.24125, 'upper': 165.9284375, 'count': 0}, {'lower': 165.9284375, 'upper': 170.61562500000002, 'count': 1}, {'lower': 170.61562500000002, 'upper': 175.30281250000002, 'count': 1}, {'lower': 175.30281250000002, 'upper': 179.99, 'count': 1}]}, 'data_type': 'float64'}, {'stats': {'match': 6, 'missing': 0, 'mismatch': 4, 'inferred_type': {'data_type': 'int', 'categorical': True}, 'frequency': [{'value': '0', 'count': 6}, {'value': '15%', 'count': 2}, {'value': '5%', 'count': 1}, {'value': '20%', 'count': 1}], 'count_uniques': 4}, 'data_type': 'object'}]
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_profile_multiple(self):
        df = self.df
        result = df.cols.profile(cols=['id', 'code', 'discount'],bins=4,flush=False)
        expected = [{'stats': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'int', 'categorical': True}, 'frequency': [{'value': 1, 'count': 1}, {'value': 2, 'count': 1}, {'value': 3, 'count': 1}, {'value': 4, 'count': 1}, {'value': 5, 'count': 1}, {'value': 6, 'count': 1}, {'value': 7, 'count': 1}, {'value': 8, 'count': 1}, {'value': 9, 'count': 1}, {'value': 10, 'count': 1}], 'count_uniques': 10}, 'data_type': 'int64'}, {'stats': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'str', 'categorical': True}, 'frequency': [{'value': 'JG15', 'count': 2}, {'value': 'L15', 'count': 1}, {'value': 'SH', 'count': 1}, {'value': 'RG30', 'count': 1}, {'value': 'J10', 'count': 1}, {'value': 'B', 'count': 1}, {'value': 'JG20', 'count': 1}, {'value': 'L20', 'count': 1}, {'value': 'FT50', 'count': 1}], 'count_uniques': 9}, 'data_type': 'object'}, {'stats': {'match': 6, 'missing': 0, 'mismatch': 4, 'inferred_type': {'data_type': 'int', 'categorical': True}, 'frequency': [{'value': '0', 'count': 6}, {'value': '15%', 'count': 2}, {'value': '5%', 'count': 1}, {'value': '20%', 'count': 1}], 'count_uniques': 4}, 'data_type': 'object'}]
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_profile_numeric(self):
        df = self.df
        result = df.cols.profile(cols='price',bins=0,flush=True)
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_quality_all(self):
        df = self.df
        result = df.cols.quality(cols='*')
        expected = {'id': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'int', 'categorical': True}}, 'name': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'str', 'categorical': True}}, 'code': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'str', 'categorical': True}}, 'price': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'decimal', 'categorical': False}}, 'discount': {'match': 6, 'missing': 0, 'mismatch': 4, 'inferred_type': {'data_type': 'int', 'categorical': True}}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_quality_multiple(self):
        df = self.df
        result = df.cols.quality(cols=['id', 'code', 'discount'],flush=False)
        expected = {'id': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'int', 'categorical': True}}, 'code': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'str', 'categorical': True}}, 'discount': {'match': 6, 'missing': 0, 'mismatch': 4, 'inferred_type': {'data_type': 'int', 'categorical': True}}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_quality_numeric(self):
        df = self.df
        result = df.cols.quality(cols='price',flush=True)
        expected = {'price': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_type': {'data_type': 'decimal', 'categorical': False}}}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_unique_values_all(self):
        df = self.df
        result = df.cols.unique_values(cols='*')
        expected = {'id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], 'name': ['pants', 'shoes', 'shirt'], 'code': ['L15', 'SH', 'RG30', 'J10', 'JG15', 'B', 'JG20', 'L20', 'FT50'], 'price': ['173.47', '69.99', '30.0', '34.99', '132.99', '57.99', '179.99', '95.0', '50.0', '169.99'], 'discount': ['0', '15%', '5%', '20%']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_unique_values_multiple(self):
        df = self.df
        result = df.cols.unique_values(cols=['id', 'code', 'discount'],estimate=False)
        expected = {'id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], 'code': ['L15', 'SH', 'RG30', 'J10', 'JG15', 'B', 'JG20', 'L20', 'FT50'], 'discount': ['0', '15%', '5%', '20%']}
        self.assertEqual(json_encoding(result), json_encoding(expected))
    
    def test_cols_unique_values_numeric(self):
        df = self.df
        result = df.cols.unique_values(cols='price',estimate=True)
        expected = ['173.47', '69.99', '30.0', '34.99', '132.99', '57.99', '179.99', '95.0', '50.0', '169.99']
        self.assertEqual(json_encoding(result), json_encoding(expected))


class TestMoreStatisticsDask(TestMoreStatisticsPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestMoreStatisticsPartitionDask(TestMoreStatisticsPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMoreStatisticsCUDF(TestMoreStatisticsPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMoreStatisticsDC(TestMoreStatisticsPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMoreStatisticsPartitionDC(TestMoreStatisticsPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


class TestMoreStatisticsSpark(TestMoreStatisticsPandas):
    config = {'engine': 'spark'}


class TestMoreStatisticsVaex(TestMoreStatisticsPandas):
    config = {'engine': 'vaex'}
