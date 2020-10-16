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
            return obj.isoformat()
            # return obj.strftime('%Y-%m-%d')

        elif isinstance(obj, (np.int32, np.int32, np.uint64,)):
            return int(obj)






def json_enconding(obj):
    """
    Encode a json. Used for testing.
    :param obj:
    :return:
    """
    return json.dumps(obj, default=json_converter)


def dump_json(value):
    return json.dumps(value, ignore_nan=True, default=json_converter)
