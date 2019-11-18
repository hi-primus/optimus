from dask.dataframe.core import DataFrame as DaskDataFrame
from glom import glom, assign
from pyspark.sql import DataFrame as SparkDataFrame

from optimus.helpers.converter import val_to_list


def meta(self):
    """
    Functions to handle dataFrames metadata
    Actions are operations like rename col, copy col... all the functions in columns.py. This used to track
    what should be recalculated in the profiler. For example a copy or rename operation do not need to
    fire a column profiling
    """

    class Meta:
        # Basic Operations
        @staticmethod
        def set(spec=None, value=None, missing=dict):
            """
            Set metadata in a dataframe columns
            :param spec: path to the key to be modified
            :param value: dict value
            :param missing:
            :return:
            """
            df = self
            if spec is not None:
                target = self.meta.get()
                data = assign(target, spec, value, missing=missing)
            else:
                data = value

            df.schema[-1].metadata = data
            return df

        @staticmethod
        def get(spec=None):
            """
            Get metadata from a dataframe column
            :param spec: path to the key to be modified
            :return:
            """
            data = self.schema[-1].metadata
            if spec is not None:
                data = glom(data, spec, skip_exc=KeyError)
            return data

        @staticmethod
        def update(path, value, default=list):
            """
            Append meta data to a key
            :param path:
            :param value:
            :param default:
            :return:
            """
            df = self
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

        # Actions
        @staticmethod
        def add_action(key, value):
            """
            Shortcut to add transformations to a dataframe
            :param key:
            :param value:
            :return:
            """
            df = self
            value = val_to_list(value)
            for v in value:
                df = df.meta.update("transformations.actions." + key, v, list)
            return df

        @staticmethod
        def copy_action(old_new_columns):
            """
            Shortcut to add transformations to a dataframe
            :param old_new_columns:
            :return:
            """

            key = "transformations.actions.copy"

            copy_cols = self.meta.get(key)
            if copy_cols is None:
                copy_cols = {}
            copy_cols.update(old_new_columns)

            df = self.meta.set(key, copy_cols)

            return df

        @staticmethod
        def rename_action(old_new_columns):
            """
            Shortcut to add rename transformations to a dataframe
            :param old_new_columns:
            :return:
            """
            key = "transformations.actions.rename"

            renamed_cols = self.meta.get(key)
            old_col, new_col = old_new_columns
            if renamed_cols is None or old_col not in list(renamed_cols.values()):
                df = self.meta.update(key, {old_col: new_col}, dict)
            else:
                # This update a key
                for k, v in renamed_cols.items():
                    n, m = old_new_columns
                    if v == n:
                        renamed_cols[k] = m

                df = self.meta.set(key, renamed_cols)
            return df

        @staticmethod
        def add_columns(value):
            """
            Shortcut to add transformations to a dataframe

            :param value:
            :return:
            """
            df = self
            value = val_to_list(value)
            for v in value:
                df = self.meta.update("transformations.columns", v, list)
            return df

        # @staticmethod
        # def append_meta(spec, value):
        #     target = self.get_meta()
        #     data = glom(target, (spec, T.append(value)))
        #
        #     df = self
        #     df.schema[-1].metadata = data
        #     return df

        @staticmethod
        def preserve(old_df, key=None, value=None):
            """
            In some cases we need to preserve metadata actions before a destructive dataframe transformation.
            :param old_df: The Spark dataframe you want to coyp the metadata
            :param key:
            :param value:
            :return:
            """
            old_meta = old_df.meta.get()
            new_meta = Meta.get()

            new_meta.update(old_meta)
            if key is None or value is None:
                return Meta.set(value=new_meta)
            else:

                return Meta.set(value=new_meta).meta.add_action(key, value)

    return Meta()


SparkDataFrame.meta = property(meta)


# This class emulate how spark metadata handling works.
class MetadataDask:
    def __init__(self):
        self._metadata = {}

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value


DaskDataFrame.schema = [MetadataDask()]

DaskDataFrame.meta = property(meta)
