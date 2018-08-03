from optimus.helpers.constants import *

import json


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


def fill_missing_var_types(var_types):
    """
    Fill missing data types with 0
    :param var_types:
    :return:
    """
    for label in PROFILER_TYPES:
        if label not in var_types:
            var_types[label] = 0
    return var_types


def sample_size(df):
    """
    Get a size sample depending on the dataframe size
    :param df:
    :return:
    """
    count = df.count()
    if count < 100:
        fraction = 0.99
    elif count < 1000:
        fraction = 0.5
    else:
        fraction = 0.1
    return fraction


# TODO: Maybe use pprint instead of this
def print_json(value):
    """
    Print beauty jsons
    :return:
    """
    print(json.dumps(value, indent=2))


def write_json(data, path):
    """
    Write a json file with the profiler result
    :param data:
    :param path:
    :return:
    """
    print(type(data))
    with open(path, 'w') as outfile:
        json.dump(data, outfile, sort_keys=True, indent=4, ensure_ascii=False)


def human_readable_bytes(num, suffix='B'):
    """
    Return a human readable file size
    :param num:
    :param suffix:
    :return:
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)
