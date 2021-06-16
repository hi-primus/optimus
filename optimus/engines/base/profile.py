from abc import ABC

from optimus.engines.base.meta import Meta
from optimus.helpers.columns import parse_columns
from optimus.helpers.core import one_list_to_val
from optimus.helpers.json import dump_json
from optimus.infer import is_list
from optimus.profiler.constants import MAX_BUCKETS


class BaseProfile(ABC):
    """Base class for all profile implementations"""

    def __init__(self, root):
        self.root = root

    def summary(self, columns="*"):

        df = self.root

        return Meta.get(df.meta, f"profile.summary")

    def columns(self, columns="*"):

        df = self.root
        columns = parse_columns(df, columns) if columns else []

        if is_list(columns):
            columns = [Meta.get(df.meta, f"profile.columns.{col}") for col in columns]
        else:
            columns = Meta.get(df.meta, f"profile.columns.{columns}")

        return one_list_to_val(columns)

    def dtypes(self, columns="*"):

        df = self.root
        columns = parse_columns(df, columns) if columns else []

        if is_list(columns):
            dtype = [Meta.get(df.meta, f"profile.columns.{col}.stats.profiler_dtype.dtype") for col in columns]
        else:
            dtype = Meta.get(df.meta, f"profile.columns.{columns}.stats.profiler_dtype.dtype")

        return one_list_to_val(dtype)

    def __call__(self, columns="*", bins: int = MAX_BUCKETS, output: str = None, flush: bool = False, size=False):
        # def profile(self, columns="*", bins: int = MAX_BUCKETS, output: str = None, flush: bool = False, size=False):
        """
        Return a dict the profile of the dataset
        :param columns:
        :param bins:
        :param output:
        :param flush:
        :param size: get the dataframe size in memory. Use with caution this could be slow for big data frames.
        :return:
        """

        df = self.root

        meta = self.root.meta
        profile = Meta.get(meta, "profile")

        if not profile:
            flush = True

        if columns or flush:
            columns = parse_columns(df, columns) if columns else []
            transformations = Meta.get(meta, "transformations") or []

            calculate = False

            if flush or len(transformations):
                calculate = True

            else:
                for col in columns:
                    if col not in profile["columns"]:
                        calculate = True

            if calculate:
                df = df[columns].calculate_profile(columns, bins, flush, size)
                profile = Meta.get(df.meta, "profile")
                self.root.meta = df.meta
            profile["columns"] = {key: profile["columns"][key] for key in columns}

        if output == "json":
            profile = dump_json(profile)

        return profile
