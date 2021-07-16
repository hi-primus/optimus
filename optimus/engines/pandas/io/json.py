import glob

import pandas as pd
import simplejson as json
from glom import glom

import optimus.helpers.functions_spark
from optimus.infer import is_dict, is_list_value, is_str, is_int

META = "_meta"
PROPERTIES = "_properties"
ITEMS = "_items"

COL_DEPTH = "depth"


class JSON:
    def __init__(self):
        self.data = None

    def load(self, path):
        """
        Load a file in JSON  format
        :param path:
        :return:
        """
        all_json = glob.glob(path, recursive=True)
        # pd.read_json("data/corona.json")
        with open(all_json[0]) as f:
            self.data = json.load(f)

    def schema(self) -> dict:
        """
        Return a dict with the count, data_type and nested structure
        :return:
        """

        def _schema(_data, _keys):
            if isinstance(_data, dict):
                for x, y in _data.items():
                    if is_dict(y):
                        _keys[x] = {
                            META: {"count": len(y), "data_type": type(y)}}
                        if len(y) > 0:
                            _keys[x][PROPERTIES] = {}
                            _schema(y, _keys[x][PROPERTIES])
                    elif is_list_value(y):
                        _keys[x] = {
                            META: {"count": len(y), "data_type": type(y)}}
                        if len(y) > 0:
                            _keys[x] = {ITEMS: {PROPERTIES: {}, META: {
                                "count": len(y), "data_type": type(y)}}}
                            _schema(y, _keys[x][ITEMS][PROPERTIES])
                    elif is_str(y):
                        _keys[x] = {
                            META: {"count": len(y), "data_type": type(y)}}
                        _schema(y, _keys[x])
                    elif is_int(y):
                        _keys[x] = {META: {"data_type": type(y)}}
                        _schema(y, _keys[x])

            elif is_list_value(_data):
                for x in _data:
                    _schema(x, _keys)

        keys = {}
        _schema(self.data, keys)
        return keys

    def freq(self, n: int = 100):
        """
        Calculate the count on every dict or list in the json
        :param n:
        :return:
        """

        def _profile(keys, parent, result=None):
            for key, values in keys.items():
                if values.get(PROPERTIES):
                    _meta = values.get(META)
                    _properties = values.get(PROPERTIES)
                elif values.get(ITEMS):
                    _meta = values.get(ITEMS).get(META)
                    _properties = values.get(ITEMS).get(PROPERTIES)

                if values.get(PROPERTIES) or values.get(ITEMS):
                    optimus.helpers.functions_spark.append(
                        [key, _meta["count"], _meta["data_type"], parent, len(parent)])
                    _profile(_properties, parent + [key], result=result)

        data = []
        _profile(self.schema(), [], data)
        pdf = pd.DataFrame(
            data, columns=['key', 'count', 'data_type', 'path', COL_DEPTH])
        return pdf.sort_values(by=["count", COL_DEPTH], ascending=[False, True]).rows.limit(n).to_dict(orient='row')

    def flatten(self, path):
        """
        Flatten a dict from a dict path
        :param path:
        :return:
        """

        def _flatten_json(_values):
            out = {}

            def flatten(x, name=''):
                if type(x) is dict:
                    for a in x:
                        flatten(x[a], name + a + '_')
                elif type(x) is list:
                    # i = 0
                    for a in x:
                        # flatten(a, name + str(i) + '_')
                        flatten(a, name + '_')
                        # i += 1
                else:
                    out[name[:-1]] = x

            flatten(_values)
            return out

        result = []
        value = glom(self.data, path, skip_exc=KeyError)
        if is_list_value(value):
            for i in value:
                result.append((_flatten_json(i)))
        elif is_dict(value):
            for i, j in value.items():
                a = {path: i}
                a.update(_flatten_json(j))
                result.append(a)
        return result

    def to_pandas(self, path):
        result = self.flatten(path)
        return pd.DataFrame(data=result)
