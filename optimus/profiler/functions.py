from optimus.helpers.constants import *
import json


# Fill missing data types with 0
def fill_missing_var_types(var_types):
    for label in TYPES_PROFILER:
        if label not in var_types:
            var_types[label] = 0
    return var_types


def sample_size(self, df):
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


def print_json(value):
    """
    Print beauty jsons
    :return:
    """
    print(json.dumps(value, indent=2))
