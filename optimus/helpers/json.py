import datetime

import simplejson as json
import pandas as pd
import numpy as np


def json_converter(obj):
    """
    Custom converter to be used with json.dumps
    :param obj:
    :return:
    """

    if not pd.isnull(obj):
        if isinstance(obj, datetime.datetime):
            # return obj.strftime('%Y-%m-%d %H:%M:%S')
            return obj.isoformat()

        elif isinstance(obj, datetime.date):
            # return obj.strftime('%Y-%m-%d')
            return obj.isoformat()

        elif isinstance(obj, (np.generic,)):
            return np.asscalar(obj)


def json_encoding(obj):
    """
    Encode a json. Used for testing.
    :param obj:
    :return:
    """
    return json.dumps(obj, default=json_converter)


def dump_json(value, *args, **kwargs):

    def _replace(data):
        if isinstance(data, dict):
            return {k: _replace(v) for k, v in data.items()}
        elif isinstance(data, (list, set, tuple,)):
            return [_replace(i) for i in data]
        elif data == float("inf"):
            return "Infinity"
        elif data == float("-inf"):
            return "-Infinity"
        else:
            return data
    return json.dumps(_replace(value), ignore_nan=True, default=json_converter, *args, **kwargs)
