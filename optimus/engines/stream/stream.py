from collections import Counter

import distogram
import pandas as pd
import requests

from optimus.engines.base.io.load import Reader
from optimus.engines.stream.commons import map_frequency, reduce_frequency, format_frequency, accum_histogram, \
    format_histogram, map_histogram


class Stream:
    def __init__(self, file_path):
        self.chunks = None
        self.col_values = None
        if (file_path):
            self.df = pd.read_csv(file_path)

        self.acc = None
        self.reduce_result = []
        self.map_result = []

    def accum(self, func_map, func_accum, col_name, chunk_size, format_results, format_params, *args, callback=None,
              **kwargs):
        # Apply an aggregation function

        for _, chunk in self.df[col_name].groupby(self.df.index // chunk_size):
            chunk = func_map(chunk)
            for value in chunk:
                self.acc = func_accum(value, *args, **kwargs)
            if callback:
                callback(format_results(self.acc, **format_params))

        # print(format_params)
        return format_results(self.acc, **format_params)

    def histogram(self, col_name, n=10, chunk_size=100, callback=None):
        format_params = {"n": n}
        h = distogram.Distogram()
        hist = self.accum(map_histogram, accum_histogram, col_name, chunk_size, format_results=format_histogram,
                          format_params=format_params, h=h, callback=callback)

        return hist

    def map(self, func_map, func_reduce, format_result, format_params, col_name, chunk_size, *args, callback=None,
            **kwargs):
        """

        :param func_map: Function
        :param func_reduce:
        :param format_result:
        :param format_params:
        :param col_name:
        :param chunk_size:
        :param args:
        :param callback:
        :param kwargs:
        :return:
        """
        self.reduce_result = None

        for _, chunk in self.df[col_name].groupby(self.df.index // chunk_size):
            # Apply the map function to every chunk
            self.map_result = func_map(chunk, *args, **kwargs)
            # Then apply the reduce function to the result of
            # the map function and the previous result of the reduce function

            self.reduce(func_reduce, *args, format_result=format_result, format_params=format_params, callback=callback,
                        **kwargs)

        return self.reduce_result

    def reduce(self, func, *args, format_result=None, format_params=None, callback=None, **kwargs):
        """

        :param func:
        :param args:
        :param format_result:
        :param format_params:
        :param callback:
        :param kwargs:
        :return:
        """

        if self.reduce_result is None:
            # print("CCC", self.reduce_result, self.map_result)
            self.reduce_result = self.map_result
        else:
            # print("DDD", self.reduce_result)
            self.reduce_result = func(self.reduce_result, self.map_result, *args, **kwargs)
        if callback:
            callback(format_result(self.reduce_result, **format_params))

    def frequency(self, col_name, n=10, chunk_size=100, callback=None):
        """

        :param col_name:
        :param n:
        :param chunk_size:
        :param callback:
        :return:
        """
        format_params = {"n": n, "col_name": col_name}
        result = self.map(map_frequency, reduce_frequency, format_frequency, format_params,
                          col_name, chunk_size, callback=callback)

        return format_frequency(result, **format_params)

    def process(self, filepath_or_buffer, n_rows=100, apply=None, *args, **kwargs):
        self.map_result = None
        self.reduce_result = None

        def callback(i, j, x, y):
            if apply:
                for func, params in apply:
                    func(self, self.df.iloc[x:y + 1], **params)
                #     Add the accumulator
                # self.df[col_name].head()
            # else:
            #     print(self.df[col_name].iloc[x:y + 1])
            # self.df[col_name].iloc[x:y + 1].head()

        with requests.get(filepath_or_buffer, params=None, stream=True) as resp:
            r = Reader(resp, 500_000, n_rows=n_rows, callback=callback)
            kwargs.pop("callback", None)
            self.df = pd.read_csv(r)

        return self.reduce_result

    @staticmethod
    def new_frequency(streamer, chunk, col_name, n=10, callback=None):
        format_params = {"n": n, "col_name": col_name}

        result = streamer.map_new(chunk[col_name], map_frequency, reduce_frequency, format_frequency, format_params,
                                  callback=callback)

        return format_frequency(result, **format_params)

    def map_new(self, chunk, func_map, func_reduce, format_result, format_params, *args, callback=None, **kwargs):

        # Apply the map function to every chunk
        self.map_result = func_map(chunk, *args, **kwargs)
        # print(self.map_result,"Aaaa")
        # Then apply the reduce function to the result of
        # the map function and the previous result of the reduce function

        self.reduce(func_reduce, *args, format_result=format_result, format_params=format_params, callback=callback,
                    **kwargs)

        return self.reduce_result


# class frequency:
#     def __init__(self, streamer):
#         format_params = {"n": n, "col_name": col_name}
#
#         result = streamer.map_new(chunk[col_name], map_frequency, reduce_frequency, format_frequency, format_params,
#                                   callback=callback)
#
#         return format_frequency(result, **format_params)
#
#     def map(self):
#         return Counter(chunk)
#
#     def reduce(self):
#         return a + b
#
#     def final(self):
#         n = kwargs["n"]
#         col_name = kwargs["col_name"]
#         # return result.most_common(n)
#         return {
#             "frequency": {
#                 col_name: {"values": [{"value": i, "count": j} for i, j in dict(result.most_common(n)).items()]}}}
