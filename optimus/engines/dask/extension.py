from collections import OrderedDict

import humanize
from dask import dataframe as dd
from dask.dataframe.core import DataFrame
from glom import assign

from optimus.engines.base.extension import BaseExt
from optimus.engines.jit import numba_histogram
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import BUFFER_SIZE
from optimus.helpers.functions import random_int, update_dict
from optimus.helpers.json import dump_json
from optimus.helpers.raiseit import RaiseIt
from optimus.profiler.constants import MAX_BUCKETS


def ext(self: DataFrame):
    class Ext(BaseExt):

        def __init__(self, df):
            super().__init__(df)

        @staticmethod
        def cache():
            df = self
            return df.persist()

        def set_buffer(self, columns, n=BUFFER_SIZE):
            df = self.df
            input_columns = parse_columns(df, columns)
            df._buffer = df[input_columns].head(n, npartitions=-1)

        @staticmethod
        def profile_new(columns, bins=MAX_BUCKETS, output=None, infer=False, flush=None):
            df = self
            from dask import delayed
            import numpy as np
            columns = parse_columns(df, columns)
            partitions = df.to_delayed()

            @delayed
            def func(pdf, _columns, _bins):
                _hist = None
                _freq = None
                for col_name in _columns:
                    series = pdf[col_name]
                    if series.dtype == np.object:
                        result = series.value_counts().nlargest(_bins).to_dict()
                    elif series.dtype == np.int64 or pdf.dtype == np.float64:

                        result = numba_histogram(series.to_numpy(), bins=_bins)
                        # _hist, bins_edges = np.histogram(pdf, bins=_bins)

                return result

            # numeric_cols = df.cols.names(columns, by_dtypes=df.constants.NUMERIC_TYPES)
            # string_cols = df.cols.names(columns, by_dtypes=df.constants.STRING_TYPES)

            _min_max = [func(part, columns, bins) for part in partitions]

            # _min_max = {col_name: dask.delayed(get_bin_edges_min_max)(**_min_max, ) for part in partitions for
            #             col_name in numeric_cols}
            # @delayed
            # def bins_col(_min_max, _bins=10):
            #     return {col_name: get_bin_edges_min_max(min, max, _bins) for col_name, (min, max) in _min_max.items()}

            # _bins = bins_col(_min_max, bins)

            # print(_bins.compute())

            # _hist = [dask.delayed(numba_histogram_edges)(part[col_name].to_numpy(), _bins[col_name]) for part in
            #          partitions for col_name in numeric_cols]

            # _bins = bins_col(columns, _min, _max)

            # result = dd.compute(_min_max)

            # delayed_parts = [func(part[col_name], [-10, 10]) for part in partitions for col_name in df.cols.names()]
            # print(dd.compute(delayed_parts))

            return _min_max

        @staticmethod
        def profile(columns, bins: int = MAX_BUCKETS, output: str = None, infer: bool = False, flush: bool = False,
                    size=False):
            """


            :param columns:
            :param bins:
            :param output:
            :param infer:
            :param flush:
            :param size: get the dataframe size ni memory. Use with caution this could be slow for big dataframes.
            :return:
            """

            df = self
            if flush is False:
                cols_to_profile = df.ext.calculate_cols_to_profile(df, columns)
                # print("cols to profile", cols_to_profile)
            else:
                cols_to_profile = parse_columns(df, columns)
            columns = cols_to_profile

            output_columns = df.meta.get("profile")
            if output_columns is None:
                output_columns = {}

            if cols_to_profile or not Ext.is_cached(df) or flush is True:
                df_length = len(df)

                numeric_cols = df.cols.names(cols_to_profile, by_dtypes=df.constants.NUMERIC_TYPES)
                string_cols = df.cols.names(cols_to_profile, by_dtypes=df.constants.STRING_TYPES)
                hist = None
                freq_uniques = None
                compute = False
                if numeric_cols is not None:
                    hist = df.cols.hist(numeric_cols, buckets=bins, compute=compute)
                    freq_uniques = df.cols.count_uniques(numeric_cols, estimate=False, compute=compute)
                freq = None
                if string_cols is not None:
                    freq = df.cols.frequency(string_cols, n=bins, count_uniques=True, compute=compute)
                # @delayed
                def merge(_columns, _hist, _freq, _mismatch, _dtypes, _freq_uniques):
                    _f = {}

                    for col_name in _columns:
                        _f[col_name] = {"stats": _mismatch[col_name], "dtype": _dtypes[col_name]}

                    if _hist is not None:
                        for col_name, h in _hist.items():
                            # _f[col_name] = {}
                            _f[col_name]["stats"]["hist"] = h["hist"]
                            _f[col_name]["stats"]["count_uniques"] = freq_uniques[col_name]["count_uniques"]

                    if _freq is not None:
                        for col_name, f in _freq.items():
                            _f[col_name]["stats"]["frequency"] = f["frequency"]
                            _f[col_name]["stats"]["count_uniques"] = f["count_uniques"]

                    return {"columns": _f}


                cols_and_inferred_dtype = df.cols.infer_profiler_dtypes(cols_to_profile)

                mismatch = df.cols.count_mismatch(cols_and_inferred_dtype, infer=True, compute=compute)
                # mismatch = df.cols

                # Nulls
                total_count_na = 0
                # for i in mismatch.values():
                #     total_count_na = total_count_na + i["missing"]

                dtypes = df.cols.dtypes("*")

                # mismatch

                hist, freq, mismatch, freq_uniques = dd.compute(hist, freq, mismatch, freq_uniques)
                updated_columns = merge(columns, hist, freq, mismatch, dtypes, freq_uniques)
                # print("(hist, freq, mismatch, freq_uniques)",(hist, freq, mismatch, freq_uniques))
                output_columns = update_dict(output_columns, updated_columns)

                # Move profiler_dtype to the parent
                # if infer is True:
                for col_name in columns:
                    output_columns["columns"][col_name].update(
                        {"profiler_dtype": output_columns["columns"][col_name]["stats"].pop("profiler_dtype")})

                assign(output_columns, "name", df.ext.get_name(), dict)
                assign(output_columns, "file_name", df.meta.get("file_name"), dict)

                data_set_info = {'cols_count': len(df.columns),
                                 'rows_count': df.rows.count(),
                                 }
                if size is True:
                    data_set_info.update({'size': df.ext.size(format="human")})

                assign(output_columns, "summary", data_set_info, dict)
                dtypes_list = list(set(df.cols.dtypes("*").values()))
                assign(output_columns, "summary.dtypes_list", dtypes_list, dict)
                assign(output_columns, "summary.total_count_dtypes", len(set([i for i in dtypes.values()])), dict)
                assign(output_columns, "summary.missing_count", total_count_na, dict)
                assign(output_columns, "summary.p_missing", round(total_count_na / df_length * 100, 2))

            actual_columns = output_columns["columns"]

            # Order columns
            output_columns["columns"] = dict(OrderedDict(
                {_cols_name: actual_columns[_cols_name] for _cols_name in df.cols.names() if
                 _cols_name in list(actual_columns.keys())}))

            df = df.meta.columns(df.cols.names())
            df.meta.set("transformations", value={})
            df.meta.set("profile", output_columns)
            # Reset Actions
            df.meta.reset()

            if output == "json":
                output_columns = dump_json(output_columns)

            return output_columns

        @staticmethod
        def sample(n=10, random=False):
            """
            Return a n number of sample from a dataFrame
            :param n: Number of samples
            :param random: if true get a semi random sample
            :return:
            """
            if random is True:
                seed = random_int()
            elif random is False:
                seed = 0
            else:
                RaiseIt.value_error(random, ["True", "False"])

            rows_count = self.rows.count()
            if n < rows_count:
                # n/rows_count can return a number that represent less the total number we expect. multiply by 1.1
                fraction = (n / rows_count) * 1.1
            else:
                fraction = 1.0
            return self.sample(frac=fraction, random_state=seed)

        @staticmethod
        def stratified_sample(col_name, seed: int = 1) -> DataFrame:
            """
            Stratified Sampling
            :param col_name:
            :param seed:
            :return:
            """
            df = self
            n = min(5, df[col_name].value_counts().min())
            df = df.groupby(col_name).apply(lambda x: x.sample(2))
            # df_.index = df_.index.droplevel(0)
            return df

        @staticmethod
        def pivot(index, column, values):
            """
            Return reshaped DataFrame organized by given index / column values.
            :param index: Column to use to make new frame's index.
            :param column: Column to use to make new frame's columns.
            :param values: Column(s) to use for populating new frame's values.
            :return:
            """
            raise NotImplementedError

        @staticmethod
        def melt(id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
            """
            Convert DataFrame from wide to long format.
            :param id_vars: column with unique values
            :param value_vars: Column names that are going to be converted to columns values
            :param var_name: Column name for vars
            :param value_name: Column name for values
            :param data_type: All columns must have the same type. It will transform all columns to this data type.
            :return:
            """

            raise NotImplementedError

        @staticmethod
        def size(deep=False, format=None):
            """
            Get the size of a dask in bytes
            :return:
            """
            df = self
            result = df.memory_usage(index=True, deep=deep).sum().compute()
            if format == "human":
                result = humanize.naturalsize(result)

            return result

        @staticmethod
        def run():
            """
            This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
            evaluation approach in processing data: transformation functions are not computed into an action is called.
            Sometimes when transformations are numerous, the computations are very extensive because the high number of
            operations that spark needs to run in order to get the results.

            Other important thing is that Apache Spark save task but not result of dataFrame, so tasks are
            accumulated and the same situation happens.

            :return:
            """

            self.cache().count()
            return self

        @staticmethod
        def query(sql_expression):
            raise NotImplementedError

        @staticmethod
        def partitions():
            df = self
            return df.npartitions

        @staticmethod
        def partitioner():
            print("Dask not support custom partitiones")
            raise NotImplementedError

        @staticmethod
        def show():
            """
            Print df lineage
            :return:
            """
            df = self
            return df.compute()

        @staticmethod
        def debug():
            """
            Print df lineage
            :return:
            """
            raise NotImplementedError

        @staticmethod
        def create_id(column="id"):
            """
            Create a unique id for every row.
            :param column: Columns to be processed
            :return:
            """

            raise NotImplementedError

    return Ext(self)


DataFrame.ext = property(ext)
