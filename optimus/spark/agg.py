from pypika import analytics as an
from pyspark.sql import functions as F

ENGINE = "spark"

agg_mapping = {
    "min": {"spark": F.min, "sql": an.Min},
    "max": {"spark": F.max, "sql": an.Max}
}

funcs = [F.min, F.max, F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance]


class Agg:
    @staticmethod
    def min():
        return agg_mapping["min"][ENGINE]

    @staticmethod
    def max():
        return agg_mapping["max"][ENGINE]

    @staticmethod
    def stddev():
        return agg_mapping["stddev"][ENGINE]

    @staticmethod
    def kurtosis():
        # https://www.periscopedata.com/blog/understanding-outliers-with-skew-and-kurtosis
        return agg_mapping["stddev"][ENGINE]

    @staticmethod
    def mean():
        return agg_mapping["mean"][ENGINE]

    @staticmethod
    def skewness():
        return agg_mapping["mean"][ENGINE]

    @staticmethod
    def sum():
        return agg_mapping["mean"][ENGINE]

    @staticmethod
    def variance():
        return agg_mapping["mean"][ENGINE]
