import copy
from collections import Counter

import numpy as np
import pandas as pd

from optimus.engines.base.meta import deepcopy
from optimus.engines.stream import distogram


class Frequency:
    def map(self, chunk):
        # Here's where you would implement your logic to calculate the frequency of the data in each chunk
        # This function should take in a chunk of the data as input and return a Counter object of frequencies
        result = Counter(chunk)
        return result

    def reduce(self, a, b):
        # Here's where you would implement your logic to combine the frequency data from each chunk
        # This function should take in a list of Counter objects (one for each chunk) and return a single
        # Counter object of the top n frequencies

        a.update(b)
        return a


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

    a.update(b)
    return a


def format_frequency(data, *args, **kwargs):
    # Create a shallow copy of the input data dictionary
    data_copy = deepcopy(data)

    # Modify the copy of the data
    n = kwargs["n"]
    cols = kwargs["cols"]

    for col_name in cols:
        # Create a new dictionary with the modified values for the column
        values = [{"value": i, "count": j} for i, j in dict(data_copy["frequency"][col_name].most_common(n)).items()]

        # Update the copy of the data with the modified column data
        data_copy["frequency"][col_name] = {"values": values}

    # Return the modified copy of the data
    return data_copy


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
