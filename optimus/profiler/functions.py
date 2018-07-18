from optimus.helpers.constants import *


# Fill missing data types with 0
def fill_missing_var_types(var_types):
    for label in TYPES_PROFILER:
        if label not in var_types:
            var_types[label] = 0
    return var_types
