from collections import Counter

import distogram
import numpy as np
import pandas as pd


def map_histogram(chunk):
    return pd.to_numeric(chunk, errors='coerce').dropna()


def map_frequency(chunk):
    # Here's where you would implement your logic to calculate the frequency of the data in each chunk
    # This function should take in a chunk of the data as input and return a Counter object of frequencies
    result = Counter(chunk)
    return result


def reduce_frequency(a, b):
    # Here's where you would implement your logic to combine the frequency data from each chunk
    # This function should take in a list of Counter objects (one for each chunk) and return a single Counter object of the top n frequencies
    result = a + b
    return result


def format_frequency(result, *args, **kwargs):
    # {'frequency': {'id': {'values': [{'value': 1, 'count': 1},
    #                                  {'value': 4907, 'count': 1},
    #                                  {'value': 4913, 'count': 1},
    #                                  {'value': 4917, 'count': 1},
    #                                  {'value': 4925, 'count': 1},
    #                                  {'value': 4926, 'count': 1},
    #                                  {'value': 4928, 'count': 1},
    #                                  {'value': 4935, 'count': 1},
    #                                  {'value': 4936, 'count': 1},
    #                                  {'value': 4937, 'count': 1}]}}}
    n = kwargs["n"]
    col_name = kwargs["col_name"]
    # return result.most_common(n)
    return {
        "frequency": {col_name: {"values": [{"value": i, "count": j} for i, j in dict(result.most_common(n)).items()]}}}


def accum_histogram(value, *args, **kwargs):
    # Here's where you would implement your logic to calculate the frequency of the data in each chunk
    # This function should take in a chunk of the data as input and return a Counter object of frequencies

    h = kwargs["h"]
    if value is not np.nan and not None:
        h = distogram.update(h, value)

    return h


def format_histogram(h, *args, **kwargs):
    # {'hist': {'id': [{'lower': 1.0, 'upper': 11227.4, 'count': 89},
    #                  {'lower': 11227.4, 'upper': 22453.8, 'count': 0},
    #                  {'lower': 22453.8, 'upper': 33680.2, 'count': 1},
    #                  {'lower': 33680.2, 'upper': 44906.6, 'count': 1},
    #                  {'lower': 44906.6, 'upper': 56133.0, 'count': 4}]}}
    #
    # nmin, nmax = distogram.bounds(h)
    # print("count: {}".format(distogram.count(h)))
    # print("mean: {}".format(distogram.mean(h)))
    # print("stddev: {}".format(distogram.stddev(h)))
    # print("min: {}".format(nmin))
    # print("5%: {}".format(distogram.quantile(h, 0.05)))
    # print("25%: {}".format(distogram.quantile(h, 0.25)))
    # print("50%: {}".format(distogram.quantile(h, 0.50)))
    # print("75%: {}".format(distogram.quantile(h, 0.75)))
    # print("95%: {}".format(distogram.quantile(h, 0.95)))
    # print("max: {}".format(nmax))

    hist_data = distogram.histogram(h, kwargs["n"])
    bins = hist_data[1]
    values = hist_data[0]
    output_data = {'hist': {'id': []}}
    for i in range(len(bins) - 1):
        output_data['hist']['id'].append({
            'lower': bins[i],
            'upper': bins[i + 1],
            'count': sum(1 for v in values if bins[i] <= v < bins[i + 1])
        })

    return output_data
