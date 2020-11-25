from glom import glom, assign

import copy

from optimus.helpers.core import val_to_list

ACTIONS_KEY = "transformations.actions"


class Meta:
    """
    Handle meta data dataframe.
    """

    def __init__(self, parent):
        self.parent = parent

        # if hasattr(self.parent, "meta_data") is False:
        #     print(hasattr(self.parent, "meta_data"))
        #     self.parent.meta_data = {}

    def reset(self):
        """
        Reset the data frame metadata
        :return:
        """
        meta = self
        meta.set(ACTIONS_KEY, [])

    def append_action(self, action, value):
        """
        Shortcut to append actions to a dataframe.
        :param action:
        :param value:
        :return: Meta
        """
        meta = copy.deepcopy(self)
        key = ACTIONS_KEY

        old_value = meta.get(key)
        if old_value is None:
            old_value = []
        old_value.append({action: value})
        meta.set(key, old_value)

        return meta

    def copy(self, old_new_columns):
        """
        Shortcut to add copy transformations to a dataframe
        :param old_new_columns:
        :return: Meta
        """

        meta = copy.deepcopy(self)
        meta = meta.append_action("copy", old_new_columns)

        return meta

    def rename(self, old_new_columns):
        """
        Shortcut to add rename transformations to a dataframe
        :param old_new_columns:
        :return: Meta
        """

        meta = self
        # assign(target, spec, value, missing=missing)
        # a = odf.meta.get()
        # if a.get("transformations") is None:
        #     odf.meta.get()["transformations"] = {}
        #
        #     if a["transformations"].get("actions") is None:
        #         odf.meta.get()["transformations"]["actions"] = {}
        #
        # odf.meta.get()["transformations"]["actions"].update({"rename":old_new_columns})
        meta = meta.append_action("rename", old_new_columns)

        return meta

    def columns(self, value):
        """
        Shortcut to cache the columns in a dataframe
        :param value:
        :return: Meta
        """
        meta = copy.deepcopy(self)
        value = val_to_list(value)
        for v in value:
            meta = meta.update("transformations.columns", v, list)
        return meta

    def action(self, name, value):
        """
        Shortcut to add actions to a dataframe
        :param name:
        :param value:
        :return: Meta
        """
        meta = copy.deepcopy(self)
        value = val_to_list(value)

        for _value in value:
            meta = meta.append_action(name, _value)

        return meta

    def preserve(self, old_df=None, key=None, value=None):
        """
        In some cases we need to preserve metadata actions before a destructive dataframe transformation.
        :param old_df: The Spark dataframe you want to copy the metadata
        :param key:
        :param value:
        :return: Meta
        """
        return self
        # old_meta = old_df.meta.get()
        # new_meta = self.parent.meta.get()
        #
        # new_meta.update(old_meta)
        # if key is None or value is None:
        #     return self.parent.meta.set(value=new_meta)
        # else:
        #
        #     return self.parent.meta.set(value=new_meta).meta.action(key, value)

    def new_preserve(self, old_odf=None, key=None, value=None):
        """
        In some cases we need to preserve metadata actions before a destructive dataframe transformation.
        :param old_odf: The Spark dataframe you want to copy the metadata
        :param key:
        :param value:
        :return: Meta
        """
        meta = copy.deepcopy(self)

        new_meta = meta.get()
        
        if old_odf is None:
            pass
        else:
            old_meta = old_odf.meta.get()
            new_meta.update(old_meta)
        
        meta.set(value=new_meta)

        if key is None:
            meta.set(value=value)
        else:
            meta.set(value=value, spec=key)

        return meta

    def update(self, path, value, default=list):
        """
        Update meta data in a key
        :param path:
        :param value:
        :param default:
        :return: Meta
        """

        meta = copy.deepcopy(self)

        new_meta = meta.get()
        if new_meta is None:
            new_meta = {}

        elements = path.split(".")
        result = new_meta
        for i, ele in enumerate(elements):
            if ele not in result and not len(elements) - 1 == i:
                result[ele] = {}

            if len(elements) - 1 == i:
                if default is list:
                    result.setdefault(ele, []).append(value)
                elif default is dict:
                    result.setdefault(ele, {}).update(value)
            else:
                result = result[ele]

        meta.set(value=new_meta)
        return meta

    def set(self, spec=None, value=None, missing=dict):
        """
        Set metadata in a dataframe columns
        :param spec: path to the key to be modified
        :param value: dict value
        :param missing:
        :return:
        """
        # df = self.parent.data
        if spec is not None:
            target = self.get()
            data = assign(target, spec, value, missing=missing)
        else:
            data = value

        self.parent.meta_data = data

    def get(self, spec=None):
        """
        Get metadata from a dataframe column
        :param spec: path to the key to be modified
        :return: dict
        """
        # print("meta", self.meta_data)
        if spec is not None:
            data = glom(self.parent.meta_data, spec, skip_exc=KeyError)
        else:
            data = self.parent.meta_data
        return data
