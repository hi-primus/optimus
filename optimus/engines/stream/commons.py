from abc import ABC, abstractmethod
from collections import Counter

import numpy as np
import pandas as pd

from optimus.engines.base.meta import deepcopy
from optimus.engines.stream import distogram


class MapReduce(ABC):
    """
    Base Class for MapReduce Algorithms
    """

    @abstractmethod
    def map(self, chunk):
        """
        Function to process a chunk of data
        :param chunk: Chunk of data
        :return:
        """
        pass

    @abstractmethod
    def reduce(self, a, b):
        """
        Function to reduce the results of the map function
        :param a: Map result
        :param b: Reduce result
        :return:
        """
        pass

    @abstractmethod
    def output_format(self, value):
        pass


class Min(MapReduce):
    def __init__(self):
        self.task_id = "min"

    def map(self, chunk):
        return np.min(chunk)

    def reduce(self, a, b):
        return min(a, b)

    def output_format(self, value):
        return value


class MinMax(MapReduce):
    def __init__(self):
        self.task_id = "min_max"

    def map(self, chunk):
        return np.min(chunk), np.max(chunk)

    def reduce(self, a, b):
        return min(a[0], b[0]), max(a[1], b[1])

    def output_format(self, value):
        return {"min": value[0], "max": value[1]}


class Max(MapReduce):
    def __init__(self):
        self.task_id = "max"

    def map(self, chunk):
        return np.max(chunk)

    def reduce(self, a, b):
        return max(a, b)

    def output_format(self, value):
        return value


class Histogram(MapReduce):
    def __init__(self, bin_size):
        self.bin_size = bin_size
        self.task_id = "histogram"
        self.h = distogram.Distogram()

    def map(self, chunk):
        chunk = pd.to_numeric(chunk, errors='coerce').dropna()

        for value in chunk:
            if value is not np.nan and not None:
                self.h = distogram.update(self.h, value)

        return self.h

    def reduce(self, a, b):
        self.h = distogram.merge(a, b)

    def output_format(self, value):
        # {'hist': {'id': [{'lower': 1.0, 'upper': 11227.4, 'count': 89},
        #                  {'lower': 11227.4, 'upper': 22453.8, 'count': 0},
        #                  {'lower': 22453.8, 'upper': 33680.2, 'count': 1},
        #                  {'lower': 33680.2, 'upper': 44906.6, 'count': 1},
        #                  {'lower': 44906.6, 'upper': 56133.0, 'count': 4}]}}

        # In case the bin size is bigger than the number of bins
        if self.bin_size > len(self.h.bins):
            self.bin_size = len(self.h.bins)
        hist_data = distogram.histogram(self.h, self.bin_size)
        bins = hist_data[1]
        values = hist_data[0]
        output_data = []
        for i in range(len(bins) - 1):
            output_data.append({
                'lower': bins[i],
                'upper': bins[i + 1],
                'count': sum(1 for v in values if bins[i] <= v < bins[i + 1])
            })

        return output_data


class Frequency(MapReduce):
    def __init__(self, n=10):
        self.n = n
        self.task_id = "frequency"

    def map(self, chunk: list):
        # Here's where you would implement your logic to calculate the frequency of the data in each chunk
        # This function should take in a chunk of the data as input and return a Counter object of frequencies
        return Counter(chunk)

    def reduce(self, a, b):
        # Here's where you would implement your logic to combine the frequency data from each chunk
        # This function should take in a list of Counter objects (one for each chunk) and return a single
        # Counter object of the top n frequencies

        a.update(b)
        return a

    def output_format(self, value):
        # Create a shallow copy of the input data dictionary
        data_copy = deepcopy(value)

        n = self.n
        values = [{"value": i, "count": j} for i, j in dict(data_copy.most_common(n)).items()]
        return {"values": values}
