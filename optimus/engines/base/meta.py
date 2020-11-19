from glom import glom, assign

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

        df = self.set(ACTIONS_KEY, [])
        return df

    def append_action(self, action, value):
        """
        Shortcut to append actions to a dataframe.
        :param action:
        :param value:
        :return:
        """
        df = self.parent
        key = ACTIONS_KEY

        old_value = df.meta.get(key)
        if old_value is None:
            old_value = []
        old_value.append({action: value})
        df = df.meta.set(key, old_value)
        return df

    def copy(self, old_new_columns):
        """
        Shortcut to add copy transformations to a dataframe
        :param old_new_columns:
        :return:
        """

        df = self.parent.data
        df = df.meta.append_action("copy", old_new_columns)

        return df

    def rename(self, old_new_columns):
        """
        Shortcut to add rename transformations to a dataframe
        :param old_new_columns:
        :return:
        """

        df = self.parent.data
        # assign(target, spec, value, missing=missing)
        # a = df.meta.get()
        # if a.get("transformations") is None:
        #     df.meta.get()["transformations"] = {}
        #
        #     if a["transformations"].get("actions") is None:
        #         df.meta.get()["transformations"]["actions"] = {}
        #
        # df.meta.get()["transformations"]["actions"].update({"rename":old_new_columns})
        df = df.meta.append_action("rename", old_new_columns)

        return df

    def columns(self, value):
        """
        Shortcut to cache the columns in a dataframe
        :param value:
        :return:
        """
        value = val_to_list(value)
        for v in value:
            df = self.update("transformations.columns", v, list)
        return df

    def action(self, name, value):
        """
        Shortcut to add actions to a dataframe
        :param name:
        :param value:
        :return:
        """
        df = self.parent.data
        value = val_to_list(value)

        for _value in value:
            df = self.parent.meta.append_action(name, _value)

        return df

    def preserve(self, old_df, key=None, value=None):
        """
        In some cases we need to preserve metadata actions before a destructive dataframe transformation.
        :param old_df: The Spark dataframe you want to copy the metadata
        :param key:
        :param value:
        :return:
        """
        pass
        # old_meta = old_df.meta.get()
        # new_meta = self.parent.meta.get()
        #
        # new_meta.update(old_meta)
        # if key is None or value is None:
        #     return self.parent.meta.set(value=new_meta)
        # else:
        #
        #     return self.parent.meta.set(value=new_meta).meta.action(key, value)

    def update(self, path, value, default=list):
        """
        Update meta data in a key
        :param path:
        :param value:
        :param default:
        :return:
        """

        df = self.parent

        new_meta = df.meta.get()
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

        df = df.meta.set(value=new_meta)
        return df

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
            target = self.parent.meta.get()
            data = assign(target, spec, value, missing=missing)
        else:
            data = value

        self.parent.meta_data = data

    def get(self, spec=None):
        """
        Get metadata from a dataframe column
        :param spec: path to the key to be modified
        :return:
        """
        # print("meta", self.meta_data)
        if spec is not None:
            data = glom(self.parent.meta_data, spec, skip_exc=KeyError)
        else:
            data = self.parent.meta_data
        return data
