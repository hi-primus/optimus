import types

import pandas as pd
import requests

from optimus.engines.base.io.reader import Reader
from optimus.engines.stream import distogram
from optimus.engines.stream.commons import map_frequency, reduce_frequency, format_frequency, accum_histogram, \
    format_histogram, map_histogram
from optimus.helpers.columns import parse_columns


class Stream:
    def __init__(self, filepath_or_buffer):
        self.chunks = None
        self.col_values = None

        # if file_path is a pandas dataframe
        # if isinstance(filepath_or_buffer, pd.DataFrame):
        self.df = filepath_or_buffer
        # elif (filepath_or_buffer):
        #     self.df = pd.read_csv(filepath_or_buffer)

        self.acc = None
        self.map_results = {}
        self.reduce_results = {}

    def accum(self, func_map, func_accum, col_name, chunk_size, format_results, format_params, *args, callback=None,
              **kwargs):
        # Apply an aggregation function
        _df = self.df.data
        for _, chunk in _df[col_name].groupby(_df.index // chunk_size):
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

    def map(self, func_list, col_name, chunk_size, *args, taskId=None, callback=None, **kwargs):
        if isinstance(func_list, types.FunctionType):
            func_list = [func_list]

        self.map_results[taskId][col_name] = []

        _df = self.df.data
        for _, chunk in _df[col_name].groupby(_df.index // chunk_size):
            for func in func_list:
                # Apply the map function to every chunk
                self.map_results[taskId][col_name] = func(chunk, *args, **kwargs)

            if callback:
                callback(col_name)

    def reduce(self, func_list, col_name, *args, taskId=None, format_result=None, format_params=None, callback=None,
               **kwargs):
        self.reduce_results[taskId][col_name] = None

        if isinstance(func_list, types.FunctionType):
            func_list = [func_list]

        function_name = taskId

        for func in func_list:

            # The first iteration will be the same map result

            if self.reduce_results[function_name][col_name] is None:
                self.reduce_results[function_name][col_name] = self.map_results[function_name][col_name]
            else:
                self.reduce_results[function_name][col_name] = func(
                    self.reduce_results[function_name][col_name], self.map_results[function_name][col_name], *args,
                    **kwargs)

            if callback:
                callback(format_result(self.reduce_results, **format_params))

    def frequency(self, cols, n=10, chunk_size=100, callback=None):
        df = self.df
        cols = parse_columns(df, cols)
        taskId = "frequency"
        self.map_results[taskId] = {}
        self.reduce_results[taskId] = {}

        format_params = {"n": n, "cols": cols}

        def callback_reduce(_col_name):
            self.reduce(reduce_frequency, _col_name, taskId=taskId, format_result=format_frequency,
                        format_params={"n": n, "cols": [_col_name]},
                        callback=callback)

        for col_name in cols:
            self.map(map_frequency, col_name, chunk_size, taskId=taskId,
                     callback=callback_reduce)

        print("self.map_results", self.reduce_results)

        return format_frequency(self.reduce_results, **format_params)

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
        """
        This is for streaming and processing multiple functions at the same time
        :param streamer:
        :param chunk:
        :param col_name:
        :param n:
        :param callback:
        :return:
        """
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
