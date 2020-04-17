from collections import OrderedDict

import humanize
from dask import delayed
from dask.dataframe.core import DataFrame
from glom import assign

from optimus.engines.base.extension import BaseExt
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.functions import random_int
from optimus.helpers.json import dump_json
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str, is_dict


def ext(self: DataFrame):
    class Ext(BaseExt):

        _name = None

        def __init__(self, df):
            super().__init__(df)

        @staticmethod
        def cache():
            df = self
            return df.persist()

        @staticmethod
        def profile(columns, bins=10, output=None, flush=None):
            """

            :param columns:
            :param bins:
            :param output:
            :return:
            """

            df = self
            df_length = len(df)

            cols_to_profile = df.ext.cols_needs_profiling(df, columns)
            columns = parse_columns(df, columns)

            output_columns = df.meta.get("profile")

            result = {}
            result["columns"] = {}

            if cols_to_profile or not Ext.is_cached(df) or flush:
                # self.rows_count = df.rows.count()
                # self.cols_count = cols_count = len(df.columns)

                numeric_cols = df.cols.names(cols_to_profile, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
                string_cols = df.cols.names(cols_to_profile, filter_by_column_dtypes=df.constants.STRING_TYPES)
                hist = None
                if numeric_cols is not None:
                    hist = df.cols.hist(numeric_cols, buckets=bins)

                freq = None
                if string_cols is not None:
                    # print("STRING COLS", string_cols)
                    # print("NNN",bins)
                    freq = df.cols.frequency(string_cols, n=bins, count_uniques=True)

                @delayed
                def merge(_columns, _hist, _freq, _mismatch, _dtypes,_freq_uniques):
                    # _h = {}
                    _f = {}

                    if _hist is not None:
                        for col_name, h in _hist.items():
                            _f[col_name] = {}
                            _f[col_name]["stats"] = mismatch[col_name]
                            _f[col_name]["stats"]["hist"] = h["hist"]
                            _f[col_name]["stats"]["count_uniques"] = freq_uniques[col_name]["count_uniques"]
                            _f[col_name]["dtype"] = _dtypes[col_name]

                    if _freq is not None:
                        for col_name, f in _freq.items():
                            _f[col_name] = {}
                            _f[col_name]["stats"] = mismatch[col_name]
                            _f[col_name]["stats"]["frequency"] = {}

                            _f[col_name]["stats"]["frequency"] = f["frequency"]
                            _f[col_name]["stats"]["count_uniques"] = f["count_uniques"]
                            _f[col_name]["dtype"] = _dtypes[col_name]

                    # _f[col_name]["stats"]["rows_count"] = _rows_count
                    return {"columns": _f}

                # Nulls
                total_count_na = 0
                mismatch = df.cols.count_mismatch({c: "int" for c in df.cols.names()})
                # print(a)
                for i in mismatch.values():
                    total_count_na = total_count_na + i["missing"]

                df.cols.count_uniques(numeric_cols)

                dtypes = df.cols.dtypes("*")

                freq_uniques  = df.cols.count_uniques(numeric_cols)
                output_columns = merge(columns, hist, freq, mismatch, dtypes,freq_uniques).compute()

                assign(output_columns, "name", df.ext.get_name(), dict)
                assign(output_columns, "file_name", df.meta.get("file_name"), dict)

                data_set_info = {'cols_count': len(df.columns),
                                 'rows_count': df.rows.count(),
                                 'size': df.ext.size(format="human")}

                assign(output_columns, "summary", data_set_info, dict)

                dtypes_list = list(set(df.cols.dtypes("*").values()))
                assign(output_columns, "summary.dtypes_list", dtypes_list, dict)
                assign(output_columns, "summary.total_count_dtypes", len(dtypes), dict)
                assign(output_columns, "summary.missing_count", total_count_na, dict)
                assign(output_columns, "summary.p_missing", round(total_count_na / df_length * 100, 2))

            actual_columns = output_columns["columns"]
            # Order columns
            output_columns["columns"] = dict(OrderedDict(
                {_cols_name: actual_columns[_cols_name] for _cols_name in df.cols.names() if
                 _cols_name in list(actual_columns.keys())}))

            df = df.meta.set(value={})
            df = df.meta.columns(df.cols.names())

            df.meta.set("profile", output_columns)

            df.meta.set("transformations.actions", {})

            if output == "json":
                output_columns = dump_json(output_columns)

            return output_columns
            # return  freq

            # "count_uniques": len(df[col_name].value_counts())})

            # for col_name in columns:
            #     stats = {}
            #
            #     # stats["stats"] = {"missing": 3, "mismatch": 4, "null": df.cols.count_na(col_name)[col_name]}
            #     stats["stats"] = df.cols.count_by_dtypes(col_name)[col_name]
            #
            #     col_dtype = df[col_name].dtype
            #     if col_dtype == np.float64 or df[col_name].dtype == np.int64:
            #         stats["stats"].update({"hist": df.cols.hist(col_name, buckets=bins)})
            #         r = {col_name: stats}
            #
            #     elif col_dtype == "object":
            #
            #         # df[col_name] = df[col_name].astype("str").dropna()
            #         stats["stats"].update({"frequency": df.cols.frequency(col_name, n=bins)[col_name],
            #                                "count_uniques": len(df[col_name].value_counts())})
            #         r = {col_name: stats}
            #     else:
            #
            #         RaiseIt.type_error(col_dtype, [np.float64, np.int64, np.object_])

            #     result["columns"].update(r)
            # result["stats"] = {"rows_count": len(df)}
            #
            # if output == "json":
            #     result = dump_json(result)

            # return

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

        @staticmethod
        def is_cached(df):
            """

            :return:
            """
            # print("is_cache", df.output_columns)
            return False if df.meta.get("profile") is None else True
            # return False

        def cols_needs_profiling(self, df, columns):
            """
            Calculate the columns that needs to be profiled.
            :return:
            """
            # Metadata
            # If not empty the profiler already run.
            # So process the dataframe's metadata to get which columns need to be profiled

            actions = df.meta.get("transformations.actions")
            are_actions = actions is not None and len(actions) > 0

            # Process actions to check if any column must be processed
            if Ext.is_cached(df):
                if are_actions:
                    drop = ["drop"]

                    def match_actions_names(_actions):
                        """
                        Get a list of columns which have been applied and specific action.
                        :param _actions:
                        :return:
                        """

                        _actions_json = df.meta.get("transformations.actions")

                        modified = []
                        for action in _actions:
                            if _actions_json.get(action):
                                # Check if was renamed
                                col = _actions_json.get(action)
                                if len(match_renames(col)) == 0:
                                    _result = col
                                else:
                                    _result = match_renames(col)
                                modified = modified + _result

                        return modified

                    def match_renames(_col_names):
                        """
                        Get a list fo columns and return the renamed version.
                        :param _col_names:
                        :return:
                        """
                        _renamed_columns = []
                        _actions = df.meta.get("transformations.actions")
                        _rename = _actions.get("rename")

                        def get_name(_col_name):
                            c = _rename.get(_col_name)
                            # The column has not been rename. Get the actual column name
                            if c is None:
                                c = _col_name
                            return c

                        if _rename:
                            # if a list
                            if is_list_of_str(_col_names):
                                for _col_name in _col_names:
                                    # The column name has been changed. Get the new name
                                    _renamed_columns.append(get_name(_col_name))
                            # if a dict
                            if is_dict(_col_names):
                                for _col1, _col2 in _col_names.items():
                                    _renamed_columns.append({get_name(_col1): get_name(_col2)})

                        else:
                            _renamed_columns = _col_names
                        return _renamed_columns

                    # New columns
                    new_columns = []

                    current_col_names = df.cols.names()
                    renamed_cols = match_renames(df.meta.get("transformations.columns"))
                    for current_col_name in current_col_names:
                        if current_col_name not in renamed_cols:
                            new_columns.append(current_col_name)

                    # Rename keys to match new names
                    profiler_columns = df.meta.get("profile")["columns"]
                    actions = df.meta.get("transformations.actions")
                    rename = actions.get("rename")
                    if rename:
                        for k, v in actions["rename"].items():
                            profiler_columns[v] = profiler_columns.pop(k)
                            profiler_columns[v]["name"] = v

                    # Drop Keys
                    for col_names in match_actions_names(drop):
                        profiler_columns.pop(col_names)

                    # Copy Keys
                    copy_columns = df.meta.get("transformations.actions.copy")
                    if copy_columns is not None:
                        for source, target in copy_columns.items():
                            profiler_columns[target] = profiler_columns[source].copy()
                            profiler_columns[target]["name"] = target
                        # Check is a new column is a copied column
                        new_columns = list(set(new_columns) - set(copy_columns.values()))

                    # Actions applied to current columns

                    modified_columns = match_actions_names(Actions.list())
                    calculate_columns = modified_columns + new_columns

                    # Remove duplicated.
                    calculate_columns = list(set(calculate_columns))

                elif not are_actions:
                    calculate_columns = None
                # elif not is_cached:
            else:
                calculate_columns = columns

            return calculate_columns

    return Ext(self)


DataFrame.ext = property(ext)
