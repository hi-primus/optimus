import math

import simplejson as json

from optimus.helpers.constants import ProfilerDataTypes, CONFIDENCE_LEVEL_CONSTANT, PROFILER_COLUMN_TYPES
from optimus.helpers.json import json_converter


def fill_missing_var_types(var_types, data_types):
    """
    Fill missing data types with 0
    :param var_types:
    :param dtypes:
    :return:
    """
    for k, v in var_types.items():
        if data_types[k] == ProfilerDataTypes.STRING.value:
            for label in ProfilerDataTypes:
                if label.value not in v:
                    var_types[k][label.value] = 0
                else:
                    var_types[k] = v
        else:
            var_types[k] = v
    return var_types


def fill_missing_col_types(col_types):
    """
    Fill missing col types with 0
    :param col_types:
    :return:
    """
    for label in PROFILER_COLUMN_TYPES:
        if label not in col_types:
            col_types[label] = 0
    return col_types


def write_json(data, path):
    """
    Write a json file with the profiler result
    :param data:
    :param path:
    :return:
    """
    try:
        with open(path, 'w', encoding='utf-8') as outfile:
            json.dump(data, outfile, indent=4, ensure_ascii=False, default=json_converter)
    except IOError:
        pass


def write_html(data, path):
    """
    Write a json file with the profiler result
    :param data:
    :param path:
    :return:
    """

    try:
        with open(path, 'w', encoding='utf-8') as outfile:
            outfile.write(data)
    except IOError:
        pass


def sample_size(population_size, confidence_level, confidence_interval):
    """
    Get a sample number of the whole population
    :param population_size:
    :param confidence_level:
    :param confidence_interval:
    :return:
    """
    z = 0.0
    p = 0.5
    e = confidence_interval / 100.0
    n = population_size

    # Loop through supported confidence levels and find the num sdd deviations for that confidence level
    for i in CONFIDENCE_LEVEL_CONSTANT:
        if i[0] == confidence_level:
            z = i[1]

    if z == 0.0:
        return -1

    # Calculate sample size
    n_0 = ((z ** 2) * p * (1 - p)) / (e ** 2)

    # Adjust sample size fo finite population
    n = n_0 / (1 + ((n_0 - 1) / float(n)))

    return int(math.ceil(n))  # sample size
