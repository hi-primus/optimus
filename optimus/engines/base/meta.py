import copy

from glom import glom, assign

from optimus.helpers.core import val_to_list

ACTIONS_KEY = "transformations.actions"


class Meta:

    @staticmethod
    def set(meta, spec=None, value=None, missing=dict) -> dict:
        """
        Set metadata in a dataframe columns
        :param meta: Meta data to be modified
        :param spec: path to the key to be modified
        :param value: dict value
        :param missing:
        :return:
        """
        if spec is not None:
            data = copy.deepcopy(meta)
            assign(data, spec, value, missing=missing)
        else:
            data = value

        return data

    @staticmethod
    def get(meta, spec=None) -> dict:
        """
        Get metadata from a dataframe column
        :param meta:Meta data to be modified
        :param spec: path to the key to be modified
        :return: dict
        """
        if spec is not None:
            data = glom(meta, spec, skip_exc=KeyError)
        else:
            data = meta
        return data

    @staticmethod
    def reset_actions(meta):
        """
        Reset the data frame metadata
        :param meta: Meta data to be modified
        :return:
        """

        return Meta.set(meta, ACTIONS_KEY, [])

    @staticmethod
    def copy(meta, old_new_columns) -> dict:
        """
        Shortcut to add copy transformations to a dataframe
        :param meta: Meta data to be modified
        :param old_new_columns:
        :return: dict
        """

        meta = copy.deepcopy(meta)
        meta = Meta.action(meta, "copy", value=old_new_columns)

        return meta

    @staticmethod
    def rename(meta, old_new_columns) -> dict:
        """
        Shortcut to add rename transformations to a dataframe
        :param meta: Meta data to be modified
        :param old_new_columns:
        :return: dict (Meta)
        """

        meta = copy.deepcopy(meta)
        meta = Meta.action(meta, "rename", value=old_new_columns)

        return meta

    @staticmethod
    def columns(meta, value) -> dict:
        """
        Shortcut to cache the columns in a dataframe
        :param meta: Meta data to be modified
        :param value:
        :return: dict (Meta)
        """
        meta = copy.deepcopy(meta)
        value = val_to_list(value)
        for v in value:
            meta = Meta.update(meta, "transformations.columns", v, list)
        return meta

    @staticmethod
    def action(meta, name, value) -> dict:
        """
        Shortcut to add actions to a dataframe
        :param meta: Meta data to be modified
        :param name: Action name
        :param value: Value to be added
        :return: dict (Meta)
        """
        meta = copy.deepcopy(meta)
        value = val_to_list(value)

        for _value in value:
            key = ACTIONS_KEY

            old_value = meta.get(key)
            if old_value is None:
                old_value = []
            old_value.append({name: value})
            meta = Meta.update(meta, key, old_value)

        return meta

    @staticmethod
    def update(meta, path, value, default=list) -> dict:
        """
        Update meta data in a key
        :param meta: Meta data to be modified
        :param path: Path indise the dict to be modified
        :param value: New key value
        :param default:
        :return: dict (Meta)
        """

        meta = copy.deepcopy(meta)

        new_meta = Meta.get(meta)
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

        Meta.set(meta, value=new_meta)
        return meta

    @staticmethod
    def preserve(meta, df=None, value=None, columns=None) -> dict:
        """
        Preserves meta
        :param meta:
        :param df:
        :param value:
        :param columns:
        :return: dict (Meta)
        """
        return meta
