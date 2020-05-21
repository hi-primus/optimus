from collections import OrderedDict

from cudf.core import DataFrame
from glom import assign

from optimus.engines.base.extension import BaseExt
from optimus.engines.dask.extension import TOTAL_PREVIEW_ROWS
from optimus.helpers.columns import parse_columns
from optimus.helpers.converter import any_dataframe_to_pandas
from optimus.helpers.functions import update_dict
from optimus.helpers.json import dump_json
from optimus.infer import Infer
from optimus.profiler.constants import MAX_BUCKETS


def ext(self: DataFrame):
    class Ext(BaseExt):
        # _name = None
        def __init__(self, df):
            super().__init__(df)

        # @staticmethod
        # def profile(columns, lower_bound, upper_bound):
        #     """
        #
        #     :param lower_bound:
        #     :param upper_bound:
        #     :param columns:
        #     :return:
        #     """
        #     df = self[lower_bound:upper_bound]
        #     # columns = parse_columns(df, columns)
        #     # result = {}
        #
        #     columns = parse_columns(df, columns)
        #     # print(df)
        #     result = {"sample": {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()]}}
        #
        #     # df = df.dropna()
        #     df = self
        #     for col_name in columns:
        #         if df[col_name].dtype == np.float64 or df[col_name].dtype == np.int64:
        #             result.update(df.cols.hist(col_name))
        #         else:
        #             # df[col_name] = df[col_name].astype("str").dropna()
        #             result.update(df.cols.frequency(col_name))
        #     return result

        @staticmethod
        def cache():
            return self

        @staticmethod
        def to_pandas():
            df = self
            return any_dataframe_to_pandas(df)

        @staticmethod
        def profile(columns, bins: int = MAX_BUCKETS, output: str = None, infer: bool = False, flush: bool = False,
                    size=True):
            """


            :param columns:
            :param bins:
            :param output:
            :param infer:
            :param flush:
            :param size:
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
                    freq_uniques = df.cols.count_uniques(numeric_cols, estimate=False)
                freq = None
                if string_cols is not None:

                    freq = df.cols.frequency(string_cols, n=bins, count_uniques=True, compute=compute)

                def merge(_columns, _hist, _freq, _mismatch, _dtypes, _freq_uniques):
                    _f = {}

                    for col_name in _columns:
                        _f[col_name] = {"stats": mismatch[col_name], "dtype": _dtypes[col_name]}

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

                # Inferred column data type using first rows
                total_preview_rows = TOTAL_PREVIEW_ROWS
                print(type(df))
                temp = df.to_pandas().head(total_preview_rows).applymap(Infer.parse_pandas)
                cols_and_inferred_dtype = {}
                for col_name in cols_to_profile:
                    _value_counts = temp[col_name].value_counts()

                    if _value_counts.index[0] != "null" and _value_counts.index[0] != "missing":
                        r = _value_counts.index[0]
                    elif _value_counts[0] < total_preview_rows:
                        r = _value_counts.index[1]
                    else:
                        r = "object"
                    cols_and_inferred_dtype[col_name] = r
                df = df.cols.profiler_dtype(columns=cols_and_inferred_dtype)
                mismatch = df.cols.count_mismatch(cols_and_inferred_dtype, infer=True)

                # Nulls
                total_count_na = 0
                for i in mismatch.values():
                    total_count_na = total_count_na + i["missing"]

                dtypes = df.cols.dtypes("*")

                # mismatch
                updated_columns = merge(columns, hist, freq, mismatch, dtypes, freq_uniques)
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
            pass

        @staticmethod
        def pivot(index, column, values):
            pass

        @staticmethod
        def melt(id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
            pass

        @staticmethod
        def query(sql_expression):
            pass

        @staticmethod
        def partitions():
            pass

        @staticmethod
        def partitioner():
            pass

        @staticmethod
        def repartition(partitions_number=None, col_name=None):
            pass

        @staticmethod
        def show():
            df = self
            return df

        @staticmethod
        def debug():
            pass

        @staticmethod
        def create_id(column="id"):
            pass

    return Ext(self)


DataFrame.ext = property(ext)
