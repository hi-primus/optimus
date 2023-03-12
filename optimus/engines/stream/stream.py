from __future__ import annotations

import pandas as pd
import requests

from optimus.engines.base.io.reader import Reader
from optimus.engines.stream.commons import Frequency, Min, Max, MinMax, Histogram, MapReduce
from optimus.helpers.columns import parse_columns
from optimus.helpers.core import val_to_list


class Stream:
    def __init__(self, filepath_or_buffer):
        self.chunks = None

        # if file_path is a pandas dataframe
        # if isinstance(filepath_or_buffer, pd.DataFrame):
        self.df = filepath_or_buffer
        # elif (filepath_or_buffer):
        #     self.df = pd.read_csv(filepath_or_buffer)

        self.acc: dict = {}
        self.map_results: dict = {}
        self.reduce_results: dict = {}

    def accum(self, func_map, func_accum, col_name, chunk_size, format_results, format_params, *args, callback=None,
              **kwargs):
        """

        :param func_map:
        :param func_accum:
        :param col_name:
        :param chunk_size:
        :param format_results:
        :param format_params:
        :param args:
        :param callback:
        :param kwargs:
        :return:
        """
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

    def map(self, func_list: list, col_name: str | list, chunk_size: int, *args, callback: callable = None, **kwargs):
        """
        Apply a map function over a chunk of data
        :param func_list: List of functions to apply to the chunk
        :param col_name:  Column name to apply the map function
        :param chunk_size: Chunk size in rows
        :param args: Arguments to pass to the map function
        :param callback: Callback function fired after each map
        :param kwargs: Keyword arguments to pass to the map function
        :return:
        """
        func_list = val_to_list(func_list)

        # Initialize the data structure for map and reduce re
        for func in func_list:
            self.map_results[func.task_id][col_name] = []
            self.reduce_results[func.task_id][col_name] = None

        _df = self.df.data
        for _, chunk in _df[col_name].groupby(_df.index // chunk_size):
            for func in func_list:
                # Apply the map function to every chunk
                self.map_results[func.task_id][col_name] = func.map(chunk, *args, **kwargs)

            if callback:
                callback(col_name)

    def reduce(self, func_list: list, col_name: str | list, *args, callback: callable = None, **kwargs):
        """
        Apply a reduce function over a map result
        :param func_list: List of functions to the map result
        :param col_name: Column name used as key to access the map result
        :param args: Arguments to pass to the reduce function
        :param callback: Callback function fired after each reduce
        :param kwargs: Keyword arguments to pass to the reduce function
        :return:
        """
        func_list = val_to_list(func_list)

        for func in func_list:

            # The first iteration will be the same map result
            task_id = func.task_id
            reduce_result = self.reduce_results[task_id][col_name]
            map_result = self.map_results[task_id][col_name]

            if reduce_result is None:
                self.reduce_results[task_id][col_name] = self.map_results[task_id][col_name]
            else:
                self.reduce_results[task_id][col_name] = func.reduce(reduce_result, map_result, *args, **kwargs)

            if callback:
                callback(func.output_format(reduce_result))

    def histogram(self, cols: str | list, bins: int = 10, chunk_size: int = 100, callback: callable = None):
        """
        Get the histogram of one or multiple columns.
        :param cols: Columns to get the histogram
        :param bins: Number of bins in the histogram
        :param chunk_size: Size of the chunk to process in rows
        :param callback: Callback function fired after each reduce
        :return:
        """
        return self.execute(cols, Histogram(bins), chunk_size=chunk_size, callback_reduce=callback)

    def frequency(self, cols: str | list, n: int = 10, chunk_size: int = 100, callback: callable = None) -> dict:
        """
        Get the count of the most frequent values of a column.
        :param cols: Columns to get the frequency
        :param n: Number of values to return
        :param chunk_size: Size of the chunk to process in rows
        :param callback: Callback function fired after each reduce
        :return:
        """
        return self.execute(cols, Frequency(n), chunk_size=chunk_size, callback_reduce=callback)

    def min(self, cols: str | list, chunk_size: int = 100, callback: callable = None) -> dict | int | float:
        """
        Get the min value of a column.
        :param cols: Columns to get the min value
        :param chunk_size: Size of the chunk to process in rows
        :param callback: Callback function fired after each reduce
        :return:
        """
        return self.execute(cols, [Min()], chunk_size=chunk_size, callback_reduce=callback)

    def max(self, cols: str | list, chunk_size: int = 100, callback: callable = None) -> dict | int | float:
        """
        Get the max value of a column.
        :param cols: Columns to get the max value
        :param chunk_size: Size of the chunk to process in rows
        :param callback: Callback function fired after each reduce
        :return:
        """
        return self.execute(cols, [Max()], chunk_size=chunk_size, callback_reduce=callback)

    def min_max(self, cols: str | list, chunk_size: int = 100, callback: callable = None) -> dict | int | float:
        """
        Get the min and max value of a column.
        :param cols: Columns to get the min and max value
        :param chunk_size: Size of the chunk to process in rows
        :param callback: Callback function fired after each reduce
        :return:
        """
        return self.execute(cols, [MinMax()], chunk_size=chunk_size, callback_reduce=callback)

    def execute(self, cols: str | list, func_list: MapReduce | list[MapReduce], chunk_size: int = 100,
                callback_map: callable = None,
                callback_reduce: callable = None):
        """
        Execute a map reduce operation using the specified functions.
        The map function is applied to every chunk of data and the reduce function is applied to the map result.
        The operation is executed like this:
        1. Map function is applied to the first chunk of data.
        2. Reduce function is applied to the map result.
        3. Map function is applied to the second chunk of data.
        4. Reduce function is applied to the map result and the previous reduce result.
        5. Repeat until all chunks are processed.
        :param cols: Columns to process
        :param func_list: List of functions to apply
        :param chunk_size: Chunk size in rows
        :param callback_map: Callback function fired after each map
        :param callback_reduce: Callback function fired after each reduce
        :return:
        """

        func_list = val_to_list(func_list)
        cols = parse_columns(self.df, cols)

        # Reset the results
        self.map_results = {}
        self.reduce_results = {}
        for func in func_list:
            self.map_results[func.task_id] = {}
            self.reduce_results[func.task_id] = {}

        def callback(_col_name):
            self.reduce(func_list, _col_name, callback=callback_reduce)

        for col_name in cols:
            self.map(func_list, col_name, chunk_size, callback=callback)

        for func in func_list:
            for col_name in cols:
                self.reduce_results[func.task_id][col_name] = func.output_format(
                    self.reduce_results[func.task_id][col_name])

        return self.reduce_results

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
