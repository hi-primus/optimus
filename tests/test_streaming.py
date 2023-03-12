import unittest

from optimus import Optimus
from optimus.engines.stream.commons import Max, Min, Frequency, Histogram

op = Optimus("pandas")
url = "https://raw.githubusercontent.com/hi-primus/optimus/develop/examples/data/foo.csv"
df = op.load.csv(url)

from optimus.engines.stream.stream import Stream

stream = Stream(df)


class TestStreaming(unittest.TestCase):
    # config = {"engine": "streaming"}
    maxDiff = None

    def test_frequency_one_column(self):
        result = stream.frequency("product", n=5, chunk_size=2, callback=None)
        expected = {'frequency': {'product': {'values': [{'value': 'pizza', 'count': 4},
                                                         {'value': 'taco', 'count': 3},
                                                         {'value': 'pasta', 'count': 2},
                                                         {'value': 'Cake', 'count': 1},
                                                         {'value': 'piza', 'count': 1}]}}}

        self.assertDictEqual(result, expected)

    def test_frequency_two_column(self):
        result = stream.frequency(["product", "price"], n=5, chunk_size=2, callback=None)
        expected = {'frequency': {'product': {'values': [{'value': 'pizza', 'count': 4},
                                                         {'value': 'taco', 'count': 3},
                                                         {'value': 'pasta', 'count': 2},
                                                         {'value': 'Cake', 'count': 1},
                                                         {'value': 'piza', 'count': 1}]},
                                  'price': {'values': [{'value': 8, 'count': 6},
                                                       {'value': 3, 'count': 4},
                                                       {'value': 10, 'count': 2},
                                                       {'value': 5, 'count': 2},
                                                       {'value': 9, 'count': 2}]}}}

        self.assertDictEqual(result, expected)

    def test_histogram_one_column(self):
        result = stream.histogram(["price"], bins=5, chunk_size=2, callback=None)
        expected = {'histogram': {'price': [{'lower': 1.0, 'upper': 2.8, 'count': 0},
                                            {'lower': 2.8, 'upper': 4.6, 'count': 0},
                                            {'lower': 4.6, 'upper': 6.4, 'count': 0},
                                            {'lower': 6.4, 'upper': 8.2, 'count': 0},
                                            {'lower': 8.2, 'upper': 10, 'count': 0}]}}

        self.assertDictEqual(result, expected)

    def test_min_one_column(self):
        result = stream.min(["price"], chunk_size=2, callback=None)
        expected = {'min': {'price': 1}}

        self.assertDictEqual(result, expected)

    def test_max_one_column(self):
        result = stream.max(["price"], chunk_size=2, callback=None)
        expected = {'max': {'price': 10}}

        self.assertDictEqual(result, expected)

    def test_min_max_one_column(self):
        result = stream.min_max(["price"], chunk_size=2, callback=None)
        expected = {'min_max': {'price': {'min': 1, 'max': 10}}}

        self.assertDictEqual(result, expected)

    def test_execute_multiple_functions(self):
        result = stream.execute("price", [Max(), Min(), Frequency(8), Histogram(10)], chunk_size=2,
                                callback_reduce=None)
        expected = {'max': {'price': 10},
                    'min': {'price': 1},
                    'frequency': {'price': {'values': [{'value': 8, 'count': 6},
                                                       {'value': 3, 'count': 4},
                                                       {'value': 10, 'count': 2},
                                                       {'value': 5, 'count': 2},
                                                       {'value': 9, 'count': 2},
                                                       {'value': 4, 'count': 1},
                                                       {'value': 2, 'count': 1},
                                                       {'value': 1, 'count': 1}]}},
                    'histogram': {'price': [{'lower': 1.0, 'upper': 2.125, 'count': 0},
                                            {'lower': 2.125, 'upper': 3.25, 'count': 0},
                                            {'lower': 3.25, 'upper': 4.375, 'count': 0},
                                            {'lower': 4.375, 'upper': 5.5, 'count': 1},
                                            {'lower': 5.5, 'upper': 6.625, 'count': 0},
                                            {'lower': 6.625, 'upper': 7.75, 'count': 0},
                                            {'lower': 7.75, 'upper': 8.875, 'count': 0},
                                            {'lower': 8.875, 'upper': 10, 'count': 0}]}}

        self.assertDictEqual(result, expected)

    def test_execute_multiple_functions_callback(self):
        result_callback = []

        def callback(value):
            return result_callback.append(value)

        stream.execute("price", [Max(), Min()], chunk_size=2,
                       callback_reduce=callback)
        expected = [{'max': {'price': 10}}, {'min': {'price': 8}}, {'max': {'price': 10}}, {'min': {'price': 8}},
                    {'max': {'price': 10}}, {'min': {'price': 5}}, {'max': {'price': 10}}, {'min': {'price': 3}},
                    {'max': {'price': 10}}, {'min': {'price': 3}}, {'max': {'price': 10}}, {'min': {'price': 3}},
                    {'max': {'price': 10}}, {'min': {'price': 3}}, {'max': {'price': 10}}, {'min': {'price': 2}},
                    {'max': {'price': 10}}, {'min': {'price': 1}}, {'max': {'price': 10}}, {'min': {'price': 1}}]

        self.assertEqual(result_callback, expected)
