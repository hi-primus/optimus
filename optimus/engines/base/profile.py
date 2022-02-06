from abc import ABC
from optimus.helpers.types import *

import time
from glom import assign
from optimus.helpers.functions import update_dict

from optimus.engines.base.meta import Meta
from optimus.helpers.columns import parse_columns
from optimus.helpers.core import one_list_to_val, val_to_list
from optimus.helpers.json import dump_json
from optimus.infer import is_list
from optimus.profiler.constants import MAX_BUCKETS


class BaseProfile(ABC):
    """Base class for all profile implementations"""

    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def summary(self, cols="*"):

        df = self.root

        return Meta.get(df.meta, "profile.summary")

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

    def __call__(self, cols="*", bins: int = MAX_BUCKETS, force_hist = None, output: str = None, flush: bool = False, size=False) -> dict:
        """
        Returns a dict the profile of the dataset
        :param cols: Columns to get the profile from
        :param bins: Number of buckets
        :param force_hist: Force histogram on selected columns
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
                df = df.profile._calculate(cols, bins, force_hist, flush, size)
                profile = Meta.get(df.meta, "profile")
                self.root.meta = df.meta
            profile["columns"] = {key: profile["columns"][key] for key in cols}

        if output == "json":
            profile = dump_json(profile)

        return profile

    def _calculate(self, cols="*", bins: int = MAX_BUCKETS, force_hist=None, flush: bool = False, size=False):
        """
        Returns a new dataframe with the profile data in its added to the meta property
        :param cols: "*", column name or list of column names to be processed.
        :param bins:
        :param flush:
        :param size: get the dataframe size in memory. Use with caution this could be slow for big data frames.
        :return:
        """
        _t = time.process_time()
        profiler_time = {"hist": {}, "frequency": {}, "count_mismatch": {}}

        df = self.root
        meta = df.meta

        force_hist = df.cols.names(force_hist) if force_hist is not None else []

        if flush is False:
            cols_to_profile = df._cols_to_profile(cols) or []
        else:
            cols_to_profile = parse_columns(df, cols) or []

        profiler_data = Meta.get(meta, "profile")

        is_cached = profiler_data is not None

        if profiler_data is None:
            profiler_data = {}
        cols_data_types = None

        profiler_time["beginning"] = {"elapsed_time": time.process_time() - _t}

        if cols_to_profile or not is_cached or flush:

            if flush:
                meta = Meta.set(meta, "profile", {})
                df.meta = meta

            hist_cols = []
            freq_cols = []

            cols_data_types = {}
            cols_to_infer = [*cols_to_profile]

            for col_name in cols_to_profile:
                col_data_type = Meta.get(df.meta, f"columns_data_types.{col_name}")

                if col_data_type is not None:
                    cols_data_types[col_name] = col_data_type
                    cols_to_infer.remove(col_name)

            if cols_to_infer:
                cols_data_types = {**cols_data_types, **df.cols.infer_type(cols_to_infer, tidy=False)["infer_type"]}
                cols_data_types = {col: cols_data_types[col] for col in cols_to_profile if col in cols_data_types}

            _t = time.process_time()
            mismatch = df.cols.quality(cols_data_types)
            profiler_time["count_mismatch"] = {
                "columns": cols_data_types, "elapsed_time": time.process_time() - _t}

            # Get with columns are numerical and does not have mismatch so we can calculate the histogram
            cols_properties = cols_data_types.items()
            for col_name, properties in cols_properties:
                if (properties.get("data_type") in df.constants.NUMERIC_TYPES
                        and not properties.get("categorical", False)) \
                        or col_name in force_hist:
                    hist_cols.append(col_name)
                else:
                    freq_cols.append(col_name)

            hist = None
            freq = {}
            sliced_freq = {}
            count_uniques = None

            if len(hist_cols) and bins>0:
                _t = time.process_time()
                hist = df.cols.hist(hist_cols, buckets=bins, compute=False)
                profiler_time["hist"] = {
                    "columns": hist_cols, "elapsed_time": time.process_time() - _t}

            if len(freq_cols) and bins>0:
                _t = time.process_time()
                sliced_cols = []
                non_sliced_cols = []

                # Extract the columns with cells larger thatn
                max_cell_length = getattr(df.meta, "max_cell_length", None)

                if max_cell_length:
                    for i, j in max_cell_length.items():
                        if i in freq_cols:
                            if j > 50:
                                sliced_cols.append(i)
                            else:
                                non_sliced_cols.append(i)

                else:
                    non_sliced_cols = freq_cols

                if len(non_sliced_cols) > 0:
                    # print("non_sliced_cols",non_sliced_cols)
                    freq = df.cols.frequency(
                        non_sliced_cols, n=bins, count_uniques=True, compute=False)

                if len(sliced_cols) > 0:
                    # print("sliced_cols", sliced_cols)
                    sliced_freq = df.cols.slice(sliced_cols, 0, 50).cols.frequency(sliced_cols, n=bins,
                                                                                   count_uniques=True,
                                                                                   compute=False)

                profiler_time["frequency"] = {
                    "columns": freq_cols, "elapsed_time": time.process_time() - _t}

            def merge(_columns, _hist, _freq, _mismatch, _data_types, _count_uniques):
                _c = {}

                _hist = {} if _hist is None else _hist.get("hist", {})
                _freq = {} if _freq is None else _freq.get("frequency", {})

                for _col_name in _columns:
                    _c[_col_name] = {
                        "stats": _mismatch.get(_col_name, None),
                        "data_type": _data_types.get(_col_name, None)
                    }
                    if _col_name in _freq:
                        f = _freq[_col_name]
                        _c[_col_name]["stats"]["frequency"] = f["values"]
                        _c[_col_name]["stats"]["count_uniques"] = f["count_uniques"]

                    elif _col_name in _hist:
                        h = _hist[_col_name]
                        _c[_col_name]["stats"]["hist"] = h

                return {"columns": _c}

            # Nulls
            total_count_na = 0

            data_types = df.cols.data_type("*", tidy=False)["data_type"]

            hist, freq, sliced_freq, mismatch = df.functions.compute(
                hist, freq, sliced_freq, mismatch)

            freq = {**freq, **sliced_freq}

            updated_columns = merge(
                cols_to_profile, hist, freq, mismatch, data_types, count_uniques)

            for col in list(profiler_data.get("columns", {}).keys()):
                if col in updated_columns["columns"]:
                    del profiler_data["columns"][col]

            profiler_data = update_dict(profiler_data, updated_columns)

            assign(profiler_data, "name", Meta.get(df.meta, "name"), dict)
            assign(profiler_data, "file_name",
                   Meta.get(df.meta, "file_name"), dict)

            data_set_info = {'cols_count': df.cols.count(),
                             'rows_count': df.rows.count(),
                             }
            if size is True:
                data_set_info.update({'size': df.size(format="human")})

            assign(profiler_data, "summary", data_set_info, dict)

            data_types_list = list(set(df.cols.data_type("*", tidy=False)["data_type"].values()))

            assign(profiler_data, "summary.data_types_list", data_types_list, dict)
            assign(profiler_data, "summary.total_count_data_types",
                   len(set([i for i in data_types.values()])), dict)
            assign(profiler_data, "summary.missing_count", total_count_na, dict)

            rows_count = df.rows.count()

            if rows_count:
                assign(profiler_data, "summary.p_missing", round(
                    total_count_na / rows_count * 100, 2))
            else:
                assign(profiler_data, "summary.p_missing", None)

        # _t = time.process_time()

        all_columns_names = df.cols.names()

        # meta = Meta.set(meta, "transformations", value={})

        # Order columns
        actual_columns = profiler_data["columns"]
        profiler_data["columns"] = {key: actual_columns[key]
                                    for key in all_columns_names if key in actual_columns}
        meta = Meta.set(meta, "profile", profiler_data)

        if cols_data_types is not None:
            df.meta = meta
            df = df.cols.set_data_type(cols_data_types, inferred=True)
            meta = df.meta

        # Reset Actions
        meta = Meta.reset_actions(meta, parse_columns(df, cols or []))
        df.meta = meta
        profiler_time["end"] = {"elapsed_time": time.process_time() - _t}
        # print(profiler_time)
        return df
