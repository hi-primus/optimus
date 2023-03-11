from abc import ABC, abstractmethod
from collections import Counter
from typing import List



class MapReduce(ABC):
    @abstractmethod
    def map(self, chunk: List[str]) -> List[tuple]:
        pass

    @abstractmethod
    def reduce(self, intermediate_results: List[tuple]) -> List[tuple]:
        pass


class FrequencyMapReduce(MapReduce):
    def map(self, cols, chunk: List[str]) -> List[tuple]:
        return Counter(chunk)

    def reduce(self, intermediate_results: List[tuple]) -> List[tuple]:
        a, b = intermediate_results
        a.update(b)
        return a


class HistogramMapReduce(MapReduce):
    def __init__(self, bin_size):
        self.bin_size = bin_size

    def map(self, chunk: List[str]) -> List[tuple]:
        return [(int(word) // self.bin_size, 1) for word in chunk]

    def reduce(self, intermediate_results: List[tuple]) -> List[tuple]:
        counts = {}
        for bin, count in intermediate_results:
            counts[bin] = counts.get(bin, 0) + count
        return list(counts.items())


class MapReduceLibrary:
    def __init__(self, map_reduce_algorithms: List[MapReduce], chunk_size: int):
        self.algorithms = map_reduce_algorithms
        self.chunk_size = chunk_size

    def execute(self, data: List[str]) -> List[tuple]:
        intermediate_results = []
        for i in range(0, len(data), self.chunk_size):
            chunk = data[i:i + self.chunk_size]
            for algorithm in self.algorithms:
                intermediate_results += algorithm.map(chunk)
            chunk_results = algorithm.reduce(intermediate_results)
            intermediate_results = []
            intermediate_results += chunk_results
        return chunk_results
