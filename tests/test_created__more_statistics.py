import datetime
import numpy as np
from optimus.tests.base import TestBase
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort, df_dicts_equal, results_equal


def Timestamp(t):
    return datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")


NaT = np.datetime64('NaT')
nan = float("nan")
inf = float("inf")


class TestMoreStatisticsPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('id', 'int64'): [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], ('name', 'object'): ['pants', 'shoes', 'shirt', 'pants', 'pants', 'shoes', 'pants', 'pants', 'shirt', 'pants'], ('code', 'object'): ['L15', 'SH', 'RG30', 'J10', 'JG15', 'B', 'JG20', 'L20', 'FT50', 'JG15'], ('price', 'float64'): [173.47, 69.99, 30.0, 34.99, 132.99, 57.99, 179.99, 95.0, 50.0, 169.99], ('discount', 'object'): ['0', '15%', '5%', '0', '0', '20%', '15%', '0', '0', '0']}
    maxDiff = None

    def test_cols_boxplot_all(self):
        df = self.df.copy()
        result = df.cols.boxplot(cols='*')
        expected = {'id': {'mean': {'id': 5.5, 'name': nan, 'code': nan, 'price': 99.441, 'discount': 0.0}, 'median': 5.5, 'q1': 3.25, 'q3': 7.75, 'whisker_low': -3.5, 'whisker_high': 14.5, 'fliers': [], 'label': 'id'}, 'name': nan, 'code': nan, 'price': {'mean': {'id': 5.5, 'name': nan, 'code': nan, 'price': 99.441, 'discount': 0.0}, 'median': 82.495, 'q1': 51.9975, 'q3': 160.74, 'whisker_low': -111.11625000000001, 'whisker_high': 323.85375, 'fliers': [], 'label': 'price'}, 'discount': {'mean': {'id': 5.5, 'name': nan, 'code': nan, 'price': 99.441, 'discount': 0.0}, 'median': 0.0, 'q1': 0.0, 'q3': 0.0, 'whisker_low': 0.0, 'whisker_high': 0.0, 'fliers': [], 'label': 'discount'}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_boxplot_multiple(self):
        df = self.df.copy()
        result = df.cols.boxplot(cols=['id', 'code', 'discount'])
        expected = {'id': {'mean': {'id': 5.5, 'code': nan, 'discount': 0.0}, 'median': 5.5, 'q1': 3.25, 'q3': 7.75, 'whisker_low': -3.5, 'whisker_high': 14.5, 'fliers': [], 'label': 'id'}, 'code': nan, 'discount': {'mean': {'id': 5.5, 'code': nan, 'discount': 0.0}, 'median': 0.0, 'q1': 0.0, 'q3': 0.0, 'whisker_low': 0.0, 'whisker_high': 0.0, 'fliers': [], 'label': 'discount'}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_boxplot_numeric(self):
        df = self.df.copy()
        result = df.cols.boxplot(cols='price')
        expected = {'price': {'mean': 99.441, 'median': 82.495, 'q1': 51.9975, 'q3': 160.74, 'whisker_low': -111.11625000000001, 'whisker_high': 323.85375, 'fliers': [], 'label': 'price'}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_correlation_all_kendall(self):
        df = self.df.copy()
        result = df.cols.correlation('*', 'kendall')
        expected = {'id': {'id': 1.0, 'price': 0.1111111111111111}, 'price': {'id': 0.1111111111111111, 'price': 1.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_correlation_all_pearson(self):
        df = self.df.copy()
        result = df.cols.correlation('*', 'pearson')
        expected = {'id': {'id': 1.0, 'price': 0.15785706335886504}, 'price': {'id': 0.15785706335886504, 'price': 1.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_correlation_all_spearman(self):
        df = self.df.copy()
        result = df.cols.correlation('*', 'spearman')
        expected = {'id': {'id': 1.0, 'price': 0.1393939393939394}, 'price': {'id': 0.1393939393939394, 'price': 1.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_correlation_multiple_kendall(self):
        df = self.df.copy()
        result = df.cols.correlation(['id', 'price'], 'kendall')
        expected = 0.1111111111111111
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_correlation_multiple_pearson(self):
        df = self.df.copy()
        result = df.cols.correlation(['id', 'price'], 'pearson')
        expected = 0.15785706335886504
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_correlation_multiple_spearman(self):
        df = self.df.copy()
        result = df.cols.correlation(['id', 'price'], 'spearman')
        expected = 0.1393939393939394
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_correlation_numeric_kendall(self):
        df = self.df.copy()
        result = df.cols.correlation('price', 'kendall')
        expected = {'price': {'price': 1.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_correlation_numeric_pearson(self):
        df = self.df.copy()
        result = df.cols.correlation('price', 'pearson')
        expected = {'price': {'price': 1.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_correlation_numeric_spearman(self):
        df = self.df.copy()
        result = df.cols.correlation('price', 'spearman')
        expected = {'price': {'price': 1.0}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_count_uniques_all(self):
        df = self.df.copy()
        result = df.cols.count_uniques(cols='*')
        expected = {'id': 10, 'name': 3, 'code': 9, 'price': 10, 'discount': 4}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_count_uniques_multiple(self):
        df = self.df.copy()
        result = df.cols.count_uniques(cols=['id', 'code', 'discount'], estimate=False)
        expected = {'id': 10, 'code': 9, 'discount': 4}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_count_uniques_numeric(self):
        df = self.df.copy()
        result = df.cols.count_uniques(cols='price', estimate=True)
        expected = 10
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_count_zeros_all(self):
        df = self.df.copy()
        result = df.cols.count_zeros(cols='*')
        expected = {'id': 0, 'name': 0, 'code': 0, 'price': 0, 'discount': 0}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_count_zeros_multiple(self):
        df = self.df.copy()
        result = df.cols.count_zeros(cols=['id', 'code', 'discount'])
        expected = {'id': 0, 'code': 0, 'discount': 0}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_count_zeros_numeric(self):
        df = self.df.copy()
        result = df.cols.count_zeros(cols='price')
        expected = 0
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_frequency_all(self):
        df = self.create_dataframe(data={('vf', 'float64'): [9.9, 9.9, 9.9, 9.9, 3.3000000000000003, 3.3000000000000003, 3.3000000000000003, 22.0, 22.0, 1.1, 1.1, 0.0, 0.0, 4.4], ('vs', 'object'): ['STR9', 'STR9', 'STR9', 'STR9', 'STR3', 'STR3', 'STR3', 'STR20', 'STR20', 'STR1', 'STR1', 'STR0', 'STR0', 'STR4'], ('values', 'int64'): [9, 9, 9, 9, 3, 3, 3, 20, 20, 1, 1, 0, 0, 4], ('o', 'object'): ['nine', [9], {'nine': 9}, 9, 3, 3, 3, None, None, 1, 1, 1, 1, 1]}, force_data_types=True)
        result = df.cols.frequency(cols='*', n=10, count_uniques=True)
        expected = {'frequency': {'vf': {'values': [{'value': 9.9, 'count': 4}, {'value': 3.3000000000000003, 'count': 3}, {'value': 0.0, 'count': 2}, {'value': 1.1, 'count': 2}, {'value': 22.0, 'count': 2}, {'value': 4.4, 'count': 1}], 'count_uniques': 6}, 'vs': {'values': [{'value': 'STR9', 'count': 4}, {'value': 'STR3', 'count': 3}, {'value': 'STR0', 'count': 2}, {'value': 'STR1', 'count': 2}, {'value': 'STR20', 'count': 2}, {'value': 'STR4', 'count': 1}], 'count_uniques': 6}, 'values': {'values': [{'value': 9, 'count': 4}, {'value': 3, 'count': 3}, {'value': 0, 'count': 2}, {'value': 1, 'count': 2}, {'value': 20, 'count': 2}, {'value': 4, 'count': 1}], 'count_uniques': 6}, 'o': {'values': [{'value': 1, 'count': 5}, {'value': 3, 'count': 3}, {'value': 9, 'count': 1}, {'value': [9], 'count': 1}, {'value': 'nine', 'count': 1}, {'value': {'nine': 9}, 'count': 1}], 'count_uniques': 6}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_frequency_multiple(self):
        df = self.create_dataframe(data={('vf', 'float64'): [9.9, 9.9, 9.9, 9.9, 3.3000000000000003, 3.3000000000000003, 3.3000000000000003, 22.0, 22.0, 1.1, 1.1, 0.0, 0.0, 4.4], ('vs', 'object'): ['STR9', 'STR9', 'STR9', 'STR9', 'STR3', 'STR3', 'STR3', 'STR20', 'STR20', 'STR1', 'STR1', 'STR0', 'STR0', 'STR4'], ('values', 'int64'): [9, 9, 9, 9, 3, 3, 3, 20, 20, 1, 1, 0, 0, 4], ('o', 'object'): ['nine', [9], {'nine': 9}, 9, 3, 3, 3, None, None, 1, 1, 1, 1, 1]}, force_data_types=True)
        result = df.cols.frequency(cols=['vs', 'vf'], n=6, percentage=True)
        expected = {'frequency': {'vs': {'values': [{'value': 'STR9', 'count': 4, 'percentage': 28.57}, {'value': 'STR3', 'count': 3, 'percentage': 21.43}, {'value': 'STR0', 'count': 2, 'percentage': 14.29}, {'value': 'STR1', 'count': 2, 'percentage': 14.29}, {'value': 'STR20', 'count': 2, 'percentage': 14.29}, {'value': 'STR4', 'count': 1, 'percentage': 7.14}]}, 'vf': {'values': [{'value': 9.9, 'count': 4, 'percentage': 28.57}, {'value': 3.3000000000000003, 'count': 3, 'percentage': 21.43}, {'value': 0.0, 'count': 2, 'percentage': 14.29}, {'value': 1.1, 'count': 2, 'percentage': 14.29}, {'value': 22.0, 'count': 2, 'percentage': 14.29}, {'value': 4.4, 'count': 1, 'percentage': 7.14}]}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_frequency_numeric(self):
        df = self.create_dataframe(data={('vf', 'float64'): [9.9, 9.9, 9.9, 9.9, 3.3000000000000003, 3.3000000000000003, 3.3000000000000003, 22.0, 22.0, 1.1, 1.1, 0.0, 0.0, 4.4], ('vs', 'object'): ['STR9', 'STR9', 'STR9', 'STR9', 'STR3', 'STR3', 'STR3', 'STR20', 'STR20', 'STR1', 'STR1', 'STR0', 'STR0', 'STR4'], ('values', 'int64'): [9, 9, 9, 9, 3, 3, 3, 20, 20, 1, 1, 0, 0, 4], ('o', 'object'): ['nine', [9], {'nine': 9}, 9, 3, 3, 3, None, None, 1, 1, 1, 1, 1]}, force_data_types=True)
        result = df.cols.frequency(cols='values', n=4, percentage=True, total_rows=3)
        expected = {'frequency': {'values': {'values': [{'value': 9, 'count': 4, 'percentage': 28.57}, {'value': 3, 'count': 3, 'percentage': 21.43}, {'value': 1, 'count': 2, 'percentage': 14.29}, {'value': 20, 'count': 2, 'percentage': 14.29}]}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_frequency_string(self):
        df = self.create_dataframe(data={('vf', 'float64'): [9.9, 9.9, 9.9, 9.9, 3.3000000000000003, 3.3000000000000003, 3.3000000000000003, 22.0, 22.0, 1.1, 1.1, 0.0, 0.0, 4.4], ('vs', 'object'): ['STR9', 'STR9', 'STR9', 'STR9', 'STR3', 'STR3', 'STR3', 'STR20', 'STR20', 'STR1', 'STR1', 'STR0', 'STR0', 'STR4'], ('values', 'int64'): [9, 9, 9, 9, 3, 3, 3, 20, 20, 1, 1, 0, 0, 4], ('o', 'object'): ['nine', [9], {'nine': 9}, 9, 3, 3, 3, None, None, 1, 1, 1, 1, 1]}, force_data_types=True)
        result = df.cols.frequency(cols='vs', n=5, percentage=False)
        expected = {'frequency': {'vs': {'values': [{'value': 'STR9', 'count': 4}, {'value': 'STR3', 'count': 3}, {'value': 'STR0', 'count': 2}, {'value': 'STR1', 'count': 2}, {'value': 'STR20', 'count': 2}]}}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_heatmap_numeric_numeric(self):
        df = self.df.copy()
        result = df.cols.heatmap(col_x='discount', col_y='price', bins_x=5, bins_y=10)
        expected = {'x': {'name': 'discount', 'edges': [-0.5, 0.5]}, 'y': {'name': 'price', 'edges': [34.99, 173.47]}, 'values': [[0.0, 0.0, 1.0, 0.0, 0.0], [0.0, 0.0, 1.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 1.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 1.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 2.0, 0.0, 0.0]]}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_heatmap_numeric_string(self):
        df = self.df.copy()
        result = df.cols.heatmap(col_x='price', col_y='code', bins_x=3, bins_y=1)
        expected = {'x': {'name': 'price', 'edges': [0.0, 1.0]}, 'y': {'name': 'code', 'edges': [0.0, 1.0]}, 'values': [[0.0, 0.0, 0.0]]}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_heatmap_string_numeric(self):
        df = self.df.copy()
        result = df.cols.heatmap(col_x='name', col_y='id', bins_x=7, bins_y=10)
        expected = {'x': {'name': 'name', 'edges': [0.0, 1.0]}, 'y': {'name': 'id', 'edges': [0.0, 1.0]}, 'values': [[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]]}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_heatmap_string_string(self):
        df = self.df.copy()
        result = df.cols.heatmap(col_x='code', col_y='name', bins_x=4, bins_y=4)
        expected = {'x': {'name': 'code', 'edges': [0.0, 1.0]}, 'y': {'name': 'name', 'edges': [0.0, 1.0]}, 'values': [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]]}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_hist_all(self):
        df = self.df.copy()
        result = df.cols.hist(cols='*', buckets=2)
        expected = {'hist': {'id': [{'lower': 1.0, 'upper': 5.5, 'count': 5}, {'lower': 5.5, 'upper': 10.0, 'count': 5}], 'price': [{'lower': 30.0, 'upper': 104.995, 'count': 6}, {'lower': 104.995, 'upper': 179.99, 'count': 4}], 'discount': [{'lower': 0.0, 'upper': 0.0, 'count': 6}]}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_hist_multiple(self):
        df = self.df.copy()
        result = df.cols.hist(cols=['id', 'code', 'discount'], buckets=4)
        expected = {'hist': {'id': [{'lower': 1.0, 'upper': 3.25, 'count': 3}, {'lower': 3.25, 'upper': 5.5, 'count': 2}, {'lower': 5.5, 'upper': 7.75, 'count': 2}, {'lower': 7.75, 'upper': 10.0, 'count': 3}], 'discount': [{'lower': 0.0, 'upper': 0.0, 'count': 6}]}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_hist_numeric(self):
        df = self.df.copy()
        result = df.cols.hist(cols='price', buckets=10)
        expected = {'hist': {'price': [{'lower': 30.0, 'upper': 44.999, 'count': 2}, {'lower': 44.999, 'upper': 59.998000000000005, 'count': 2}, {'lower': 59.998000000000005, 'upper': 74.997, 'count': 1}, {'lower': 74.997, 'upper': 89.99600000000001, 'count': 0}, {'lower': 89.99600000000001, 'upper': 104.995, 'count': 1}, {'lower': 104.995, 'upper': 119.994, 'count': 0}, {'lower': 119.994, 'upper': 134.993, 'count': 1}, {'lower': 134.993, 'upper': 149.99200000000002, 'count': 0}, {'lower': 149.99200000000002, 'upper': 164.991, 'count': 0}, {'lower': 164.991, 'upper': 179.99, 'count': 3}]}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_infer_type_all(self):
        df = self.df.copy()
        result = df.cols.infer_type(cols='*')
        expected = {'id': {'data_type': 'int', 'categorical': True}, 'name': {'data_type': 'str', 'categorical': True}, 'code': {'data_type': 'str', 'categorical': True}, 'price': {'data_type': 'float', 'categorical': True}, 'discount': {'data_type': 'int', 'categorical': True}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_infer_type_multiple(self):
        df = self.df.copy()
        result = df.cols.infer_type(cols=['id', 'code', 'discount'])
        expected = {'id': {'data_type': 'int', 'categorical': True}, 'code': {'data_type': 'str', 'categorical': True}, 'discount': {'data_type': 'int', 'categorical': True}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_infer_type_numeric(self):
        df = self.df.copy()
        result = df.cols.infer_type(cols='price')
        expected = {'data_type': 'float', 'categorical': True}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_profile_all(self):
        df = self.create_dataframe(data={('vf', 'float64'): [9.9, 9.9, 9.9, 9.9, 3.3000000000000003, 3.3000000000000003, 3.3000000000000003, 22.0, 22.0, 1.1, 1.1, 0.0, 0.0, 4.4], ('vs', 'object'): ['STR9', 'STR9', 'STR9', 'STR9', 'STR3', 'STR3', 'STR3', 'STR20', 'STR20', 'STR1', 'STR1', 'STR0', 'STR0', 'STR4'], ('values', 'int64'): [9, 9, 9, 9, 3, 3, 3, 20, 20, 1, 1, 0, 0, 4], ('o', 'object'): ['nine', [9], {'nine': 9}, 9, 3, 3, 3, None, None, 1, 1, 1, 1, 1]}, force_data_types=True)
        result = df.cols.profile(cols='*')
        expected = [{'stats': {'match': 14, 'missing': 0, 'mismatch': 0, 'inferred_data_type': {'data_type': 'float', 'categorical': True}, 'frequency': [{'value': 9.9, 'count': 4}, {'value': 3.3000000000000003, 'count': 3}, {'value': 0.0, 'count': 2}, {'value': 1.1, 'count': 2}, {'value': 22.0, 'count': 2}, {'value': 4.4, 'count': 1}], 'count_uniques': 6}, 'data_type': 'float64'}, {'stats': {'match': 14, 'missing': 0, 'mismatch': 0, 'inferred_data_type': {'data_type': 'str', 'categorical': True}, 'frequency': [{'value': 'STR9', 'count': 4}, {'value': 'STR3', 'count': 3}, {'value': 'STR0', 'count': 2}, {'value': 'STR1', 'count': 2}, {'value': 'STR20', 'count': 2}, {'value': 'STR4', 'count': 1}], 'count_uniques': 6}, 'data_type': 'object'}, {'stats': {'match': 14, 'missing': 0, 'mismatch': 0, 'inferred_data_type': {'data_type': 'int', 'categorical': True}, 'frequency': [{'value': 9, 'count': 4}, {'value': 3, 'count': 3}, {'value': 0, 'count': 2}, {'value': 1, 'count': 2}, {'value': 20, 'count': 2}, {'value': 4, 'count': 1}], 'count_uniques': 6}, 'data_type': 'int64'}, {'stats': {'match': 9, 'missing': 2, 'mismatch': 3, 'inferred_data_type': {'data_type': 'int', 'categorical': True}, 'frequency': [{'value': 1, 'count': 5}, {'value': 3, 'count': 3}, {'value': 9, 'count': 1}, {'value': [9], 'count': 1}, {'value': 'nine', 'count': 1}, {'value': {'nine': 9}, 'count': 1}], 'count_uniques': 6}, 'data_type': 'object'}]
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_profile_multiple(self):
        df = self.create_dataframe(data={('vf', 'float64'): [9.9, 9.9, 9.9, 9.9, 3.3000000000000003, 3.3000000000000003, 3.3000000000000003, 22.0, 22.0, 1.1, 1.1, 0.0, 0.0, 4.4], ('vs', 'object'): ['STR9', 'STR9', 'STR9', 'STR9', 'STR3', 'STR3', 'STR3', 'STR20', 'STR20', 'STR1', 'STR1', 'STR0', 'STR0', 'STR4'], ('values', 'int64'): [9, 9, 9, 9, 3, 3, 3, 20, 20, 1, 1, 0, 0, 4], ('o', 'object'): ['nine', [9], {'nine': 9}, 9, 3, 3, 3, None, None, 1, 1, 1, 1, 1]}, force_data_types=True)
        result = df.cols.profile(cols=['vs', 'vf'], bins=8, flush=False)
        expected = [{'stats': {'match': 14, 'missing': 0, 'mismatch': 0, 'inferred_data_type': {'data_type': 'str', 'categorical': True}, 'frequency': [{'value': 'STR9', 'count': 4}, {'value': 'STR3', 'count': 3}, {'value': 'STR0', 'count': 2}, {'value': 'STR1', 'count': 2}, {'value': 'STR20', 'count': 2}, {'value': 'STR4', 'count': 1}], 'count_uniques': 6}, 'data_type': 'object'}, {'stats': {'match': 14, 'missing': 0, 'mismatch': 0, 'inferred_data_type': {'data_type': 'float', 'categorical': True}, 'frequency': [{'value': 9.9, 'count': 4}, {'value': 3.3000000000000003, 'count': 3}, {'value': 0.0, 'count': 2}, {'value': 1.1, 'count': 2}, {'value': 22.0, 'count': 2}, {'value': 4.4, 'count': 1}], 'count_uniques': 6}, 'data_type': 'float64'}]
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_profile_numeric(self):
        df = self.create_dataframe(data={('vf', 'float64'): [9.9, 9.9, 9.9, 9.9, 3.3000000000000003, 3.3000000000000003, 3.3000000000000003, 22.0, 22.0, 1.1, 1.1, 0.0, 0.0, 4.4], ('vs', 'object'): ['STR9', 'STR9', 'STR9', 'STR9', 'STR3', 'STR3', 'STR3', 'STR20', 'STR20', 'STR1', 'STR1', 'STR0', 'STR0', 'STR4'], ('values', 'int64'): [9, 9, 9, 9, 3, 3, 3, 20, 20, 1, 1, 0, 0, 4], ('o', 'object'): ['nine', [9], {'nine': 9}, 9, 3, 3, 3, None, None, 1, 1, 1, 1, 1]}, force_data_types=True)
        result = df.cols.profile(cols='values', bins=10, flush=True)
        expected = {'stats': {'match': 14, 'missing': 0, 'mismatch': 0, 'inferred_data_type': {'data_type': 'int', 'categorical': True}, 'frequency': [{'value': 9, 'count': 4}, {'value': 3, 'count': 3}, {'value': 0, 'count': 2}, {'value': 1, 'count': 2}, {'value': 20, 'count': 2}, {'value': 4, 'count': 1}], 'count_uniques': 6}, 'data_type': 'int64'}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_quality_all(self):
        df = self.df.copy()
        result = df.cols.quality(cols='*')
        expected = {'id': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_data_type': 'int'}, 'name': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_data_type': 'str'}, 'code': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_data_type': 'str'}, 'price': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_data_type': 'float'}, 'discount': {'match': 6, 'missing': 0, 'mismatch': 4, 'inferred_data_type': 'int'}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_quality_multiple(self):
        df = self.df.copy()
        result = df.cols.quality(cols=['id', 'code', 'discount'], flush=False)
        expected = {'id': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_data_type': 'int'}, 'code': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_data_type': 'str'}, 'discount': {'match': 6, 'missing': 0, 'mismatch': 4, 'inferred_data_type': 'int'}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_quality_numeric(self):
        df = self.df.copy()
        result = df.cols.quality(cols='price', flush=True)
        expected = {'price': {'match': 10, 'missing': 0, 'mismatch': 0, 'inferred_data_type': 'float'}}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_unique_values_all(self):
        df = self.df.copy()
        result = df.cols.unique_values(cols='*')
        expected = {'id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], 'name': ['pants', 'shoes', 'shirt'], 'code': ['L15', 'SH', 'RG30', 'J10', 'JG15', 'B', 'JG20', 'L20', 'FT50'], 'price': ['173.47', '69.99', '30.0', '34.99', '132.99', '57.99', '179.99', '95.0', '50.0', '169.99'], 'discount': ['0', '15%', '5%', '20%']}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_unique_values_multiple(self):
        df = self.df.copy()
        result = df.cols.unique_values(cols=['id', 'code', 'discount'], estimate=False)
        expected = {'id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], 'code': ['L15', 'SH', 'RG30', 'J10', 'JG15', 'B', 'JG20', 'L20', 'FT50'], 'discount': ['0', '15%', '5%', '20%']}
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))

    def test_cols_unique_values_numeric(self):
        df = self.df.copy()
        result = df.cols.unique_values(cols='price', estimate=True)
        expected = ['173.47', '69.99', '30.0', '34.99', '132.99', '57.99', '179.99', '95.0', '50.0', '169.99']
        self.assertTrue(results_equal(result, expected, decimal=5, assertion=True))


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


try:
    import pyspark # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMoreStatisticsSpark(TestMoreStatisticsPandas):
        config = {'engine': 'spark'}


try:
    import vaex # pyright: reportMissingImports=false
except:
    pass
else:
    class TestMoreStatisticsVaex(TestMoreStatisticsPandas):
        config = {'engine': 'vaex'}
