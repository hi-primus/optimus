from functools import reduce, partial

import pandas as pd


class Stream:
    def __init__(self, file_path):
        self.chunks = None
        self.col_values = None
        self.df = pd.read_csv(file_path)
        self.map_results = []
        self.acc = None
        self.result_streaming = []
        self.new_map_chunk = []

    def accum(self, func, col_name, chunk_size, *args, callback=None, **kwargs):
        # Apply an aggregation function

        for _, chunk in self.df[col_name].groupby(self.df.index // chunk_size):
            callback("chunk")
            for value in chunk:
                self.acc = func(value, *args, **kwargs)
        return self.acc

    def map(self, func, col_name, chunk_size, *args, callback=None, **kwargs):

        self.map_results = [func(chunk, *args, callback, **kwargs) for _, chunk in
                            self.df[col_name].groupby(self.df.index // chunk_size)]
        return self

    def reduce(self, func, *args, format=None, callback=None, **kwargs):
        # Apply a reduce function
        new_function = partial(func, callback=callback, format=format)
        result = reduce(new_function, self.map_results)
        if format:
            result = format(reduce(new_function, self.map_results))
        if callback:
            callback(result)
        return result

    def frequency(self, col_name, n=10, chunk_size=100, callback_mapper=None, callback_reducer=None):

        # Apply an aggregation function

        result = self \
            .map(self.map_frequency, col_name, chunk_size, callback=callback_mapper) \
            .reduce(self.reduce_frequency, format=None, callback=callback_reducer).most_common(n)

        return result
