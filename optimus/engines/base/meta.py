import copy

from glom import glom, assign, delete

from optimus.helpers.core import val_to_list
from optimus.infer import is_dict, is_list_value

ACTIONS_PATH = "transformations.actions"


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
    def reset(meta, spec=None) -> dict:
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
            delete(data, spec, ignore_missing=True)
        else:
            data = meta

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
        return copy.deepcopy(data)

    @staticmethod
    def reset_actions(meta, cols="*"):
        """
        Reset the data frame metadata
        :param meta: Meta data to be modified
        :return:
        """

        if cols == "*":
            return Meta.set(meta, ACTIONS_PATH, [])
        else:
            actions = Meta.get(meta, ACTIONS_PATH) or []
            actions = [action for action in actions if action["columns"] not in (cols or [])]
            return Meta.set(meta, ACTIONS_PATH, actions)


    @staticmethod
    def columns(meta, value) -> dict:
        """
        Shortcut to cache the columns in a dataframe
        :param meta: Meta data to be modified
        :param value:
        :return: dict (Meta)
        """
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
        if not is_list_value(value):
            value = [value]
        for v in value:
            meta = Meta.update(meta, ACTIONS_PATH, {"name": name, "columns": v}, list)
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

        new_meta = copy.deepcopy(meta)

        if new_meta is None:
            new_meta = {}

        elements = path.split(".")
        _element = new_meta
        for i, ele in enumerate(elements):
            if ele not in _element and not len(elements) - 1 == i:
                _element[ele] = {}

            if len(elements) - 1 == i:
                if default is list:
                    _element[ele] = _element.get(ele, [])
                    _element[ele].append(value)
                elif default is dict:
                    _element[ele] = _element.get(ele, {})
                    _element[ele].update(value)
            else:
                _element = _element[ele]

        return new_meta

    @staticmethod
    def drop_columns(meta, cols):
        for col in cols:
            meta = Meta.reset(meta, f'columns_data_types.{col}')
            meta = Meta.reset(meta, f'max_cell_length.{col}')
            meta = Meta.reset(meta, f'profile.columns.{col}')
        return meta

    @staticmethod
    def select_columns(meta, cols):
        all_cols = []

        for _cols in ['columns_data_types', 'max_cell_length', 'profile.columns']:
            found = Meta.get(meta, _cols)
            if is_dict(found):
                all_cols += list(found.keys())

        to_drop = list(set(all_cols) - set(cols))
        return Meta.drop_columns(meta, to_drop)