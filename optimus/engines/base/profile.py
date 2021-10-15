from abc import ABC
from optimus.helpers.types import *

from optimus.engines.base.meta import Meta
from optimus.helpers.columns import parse_columns
from optimus.helpers.core import one_list_to_val
from optimus.helpers.json import dump_json
from optimus.infer import is_list
from optimus.profiler.constants import MAX_BUCKETS


class BaseProfile(ABC):
    """Base class for all profile implementations"""

    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def summary(self, cols="*"):

        df = self.root

        return Meta.get(df.meta, f"profile.summary")

    def columns(self, cols="*"):

        df = self.root
        cols = parse_columns(df, cols) if cols else []

        if is_list(cols):
            cols = [Meta.get(df.meta, f"profile.columns.{col}") for col in cols]
        else:
            cols = Meta.get(df.meta, f"profile.columns.{cols}")

        return one_list_to_val(cols)

    def data_type(self, cols="*"):

        df = self.root
        cols = parse_columns(df, cols) if cols else []

        dtype = [Meta.get(
            df.meta, f"profile.columns.{col}.stats.inferred_data_type.data_type") for col in cols]

        return one_list_to_val(dtype)

    def __call__(self, cols="*", bins: int = MAX_BUCKETS, output: str = None, flush: bool = False, size=False) -> dict:
        """
        Returns a dict the profile of the dataset
        :param cols: Columns to get the profile from
        :param bins: Number of buckets
        :param output:
        :param flush: Flushes the cache of the whole profile to process it again
        :param size: get the dataframe size in memory. Use with caution this could be slow for big data frames.
        :return:
        """

        df = self.root

        meta = self.root.meta
        profile = Meta.get(meta, "profile")

        if not profile:
            flush = True

        if cols or flush:
            cols = parse_columns(df, cols) if cols else []
            transformations = Meta.get(meta, "transformations") or []

            calculate = False

            if flush or len(transformations):
                calculate = True

            else:
                for col in cols:
                    if col not in profile["columns"]:
                        calculate = True

            if calculate:
                df = df.calculate_profile(cols, bins, flush, size)
                profile = Meta.get(df.meta, "profile")
                self.root.meta = df.meta
            profile["columns"] = {key: profile["columns"][key] for key in cols}

        if output == "json":
            profile = dump_json(profile)

        return profile
