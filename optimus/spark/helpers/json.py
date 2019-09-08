import datetime
import json


def json_converter(obj):
    """
    Custom converter to be used with json.dumps
    :param obj:
    :return:
    """
    if isinstance(obj, datetime.datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')

    elif isinstance(obj, datetime.date):
        return obj.strftime('%Y-%m-%d')


def json_enconding(obj):
    """
    Encode a json. Used for testing.
    :param obj:
    :return:
    """
    return json.dumps(obj, default=json_converter)