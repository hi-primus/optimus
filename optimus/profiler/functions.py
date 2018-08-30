import json
import math
import timeit

from pyspark.sql import functions as F
from pyspark.sql.functions import when

from optimus.helpers.constants import *
from optimus.helpers.functions import parse_columns

confidence_level_constant = [50, .67], [68, .99], [90, 1.64], [95, 1.96], [99, 2.57]


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
    try:
        with open(path, 'w', encoding='utf-8') as outfile:
            json.dump(data, outfile, sort_keys=True, indent=4, ensure_ascii=False)
    except IOError:
        pass


def human_readable_bytes(value, suffix='B'):
    """
    Return a human readable file size
    :param value:
    :param suffix:
    :return:
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(value) < 1024.0:
            return "%3.1f%s%s" % (value, unit, suffix)
        value /= 1024.0
    return "%.1f%s%s" % (value, 'Yi', suffix)


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
    for i in confidence_level_constant:
        if i[0] == confidence_level:
            z = i[1]

    if z == 0.0:
        return -1

    # Calculate sample size
    n_0 = ((z ** 2) * p * (1 - p)) / (e ** 2)

    # Adjust sample size fo finite population
    n = n_0 / (1 + ((n_0 - 1) / float(n)))

    return int(math.ceil(n))  # sample size


def bucketizer(df, columns, splits):
    """

    :param df:
    :param columns:
    :param splits:
    :return:
    """
    start_time = timeit.default_timer()
    columns = parse_columns(df, columns)

    def _bucketizer(col_name, args):
        """
        Create a column expression that create buckets in a range of values
        :param col_name: Column to be processed
        :return:
        """
        out_in_columns = args[1]
        col_name_input = out_in_columns[col_name]

        buckets = args[0]

        expr = None
        i = 0

        # TODO: seems that this can be written with reduce
        for b in buckets:
            if i == 0:
                expr = when((F.col(col_name_input) >= b["lower"]) & (F.col(col_name_input) <= b["upper"]), b["bucket"])
            else:
                expr = expr.when((F.col(col_name_input) >= b["lower"]) & (F.col(col_name_input) <= b["upper"]),
                                 b["bucket"])
            i = i + 1

        return expr

    output_columns = [c + "_buckets" for c in columns]

    # TODO: This seems weird but I can not find another way. Send the actual column name to the func not seems right
    df = df.cols.apply_expr(output_columns, _bucketizer, [splits, dict(zip(output_columns, columns))])
    logging.info("bucketizer")
    logging.info(timeit.default_timer() - start_time)
    return df


def create_buckets(low_val, high_val, bins):
    """
    Create a dictionary with bins
    :param low_val: low range
    :param high_val: high range
    :param buckets: number of buckets
    :return:
    """
    range_value = (high_val - low_val) / bins
    low = low_val

    buckets = []
    for i in range(0, bins):
        high = low + range_value
        buckets.append({"lower": low, "upper": high, "bucket": i})
        low = high
    return buckets
