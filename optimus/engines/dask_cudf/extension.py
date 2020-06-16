import math
from collections import OrderedDict

import imgkit
import numpy as np
from dask import delayed
from dask_cudf.core import DataFrame
from glom import assign

from optimus.engines.base.extension import BaseExt
from optimus.engines.dask.extension import TOTAL_PREVIEW_ROWS
from optimus.helpers.columns import parse_columns
from optimus.helpers.converter import any_dataframe_to_pandas
from optimus.helpers.functions import random_int, absolute_path, update_dict
from optimus.helpers.functions_spark import traverse
from optimus.helpers.json import dump_json
from optimus.helpers.output import print_html
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import Infer, is_list_of_str, is_dict
from optimus.profiler.constants import MAX_BUCKETS


def ext(self):
    class Ext(BaseExt):

        # _name = None

        @staticmethod
        def cache():
            return self  # Dask.instance.persist(self)

        @staticmethod
        def profile(columns, bins: int = MAX_BUCKETS, output: str = None, flush: bool = False,
                    size=False):
            """

            :param columns:
            :param bins:
            :param output:
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

                @delayed
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
                if infer is True:
                    total_preview_rows = TOTAL_PREVIEW_ROWS
                    # print(type(df))
                    temp = any_dataframe_to_pandas(df.head(total_preview_rows)).applymap(Infer.parse_pandas)
                    cols_to_infer = {}
                    for col_name in columns:
                        _value_counts = temp[col_name].value_counts()

                        if _value_counts.index[0] != "null" and _value_counts.index[0] != "missing":
                            r = _value_counts.index[0]
                        elif _value_counts[0] < total_preview_rows:
                            r = _value_counts.index[1]
                        else:
                            r = "object"

                        cols_to_infer[col_name] = r
                else:
                    cols_to_infer = df.cols.profiler_dtypes(cols_to_profile)

                mismatch = df.cols.count_mismatch(cols_to_infer)

                # Nulls
                total_count_na = 0
                for i in mismatch.values():
                    total_count_na = total_count_na + i["missing"]

                dtypes = df.cols.dtypes("*")

                # mismatch
                updated_columns = merge(columns, hist, freq, mismatch, dtypes, freq_uniques).compute()
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
        def set_name(value=None):
            """
            Create a temp view for a data frame also used in the json output profiling
            :param value:
            :return:
            """
            self.ext._name = value
            # if not is_str(value):
            #     RaiseIt.type_error(value, ["string"])

            # if len(value) == 0:
            #     RaiseIt.value_error(value, ["> 0"])


        @staticmethod
        def get_name():
            """
            Get dataframe name
            :return:
            """
            return self.ext._name

        @staticmethod
        def partitions():
            return self.npartitions

        @staticmethod
        def partitioner():
            raise NotImplementedError

        #
        # @staticmethod
        # def repartition(n=0, col_name=None):
        #     return df.repartition(n)

        @staticmethod
        def table_image(path, limit=10):
            """
            Output table as image
            :param limit:
            :param path:
            :return:
            """

            css = absolute_path("/css/styles.css")

            imgkit.from_string(Ext.table_html(limit=limit, full=True), path, css=css)
            print_html("<img src='" + path + "'>")

        @staticmethod
        def show():
            """
            Print df lineage
            :return:
            """
            return self.compute()

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
            return False if df.meta.get("profile") is None else True

        def calculate_cols_to_profile(self, df, columns):
            """
            Calculate the columns that needs to be profiled.
            :return:
            """
            # Metadata
            # If not empty the profiler already run.
            # So process the dataframe's metadata to get which columns need to be profiled

            actions = df.meta.get("transformations.actions")
            are_actions = actions is not None and len(actions) > 0

            # print("are actions", are_actions)

            def _get_columns(action):
                """
                Get the column applied to the specified action
                :param action:
                :return:
                """
                _actions = df.meta.get("transformations.actions")
                result = None
                if _actions:
                    result = [j for i in _actions for col_name, j in i.items() if col_name == action]
                return result

            # Process actions to check if any column must be processed
            if Ext.is_cached(df):
                if are_actions:

                    def get_columns_by_action(action):
                        """
                        Get a list of columns which have been applied and specific action.
                        :param action:
                        :return:
                        """
                        modified = []

                        col = _get_columns(action)
                        # Check if was renamed
                        if len(get_renamed_columns(col)) == 0:
                            _result = col
                        else:
                            _result = get_renamed_columns(col)
                        modified = modified + _result

                        return modified

                    def get_renamed_columns(_col_names):
                        """
                        Get a list of columns and return the renamed version.
                        :param _col_names:
                        :return:
                        """
                        _renamed_columns = []

                        _rename = _get_columns("rename")

                        # print("RENAME", _rename)

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
                    profiler_columns = df.meta.get("profile.columns")

                    # Operations need to be processed int the same order that created
                    modified_columns = []
                    for l in df.meta.get("transformations.actions"):
                        for action_name, j in l.items():
                            if action_name == "copy":
                                for source, target in j.items():
                                    profiler_columns[target] = profiler_columns[source].copy()
                                    profiler_columns[target]["name"] = target
                                # Check is a new column is a copied column
                                new_columns = list(set(new_columns) - set(j.values()))

                            # Rename keys to match new names
                            elif action_name == "rename":
                                renamed_cols = get_renamed_columns(df.meta.get("transformations.columns"))
                                for current_col_name in current_col_names:
                                    if current_col_name not in renamed_cols:
                                        new_columns.append(current_col_name)

                                rename = _get_columns("rename")
                                if rename:
                                    for l in rename:
                                        for k, v in l.items():
                                            profiler_columns[v] = profiler_columns.pop(k)
                                            profiler_columns[v]["name"] = v

                            # Drop Keys
                            elif action_name == "drop":
                                for col_names in get_columns_by_action(action_name):
                                    profiler_columns.pop(col_names)
                            else:
                                # print("ACTION NAME", action_name)
                                modified_columns = modified_columns + (get_columns_by_action(action_name))

                    # Actions applied to current columns
                    # print("modified_columns", modified_columns)
                    # print("new_columns", new_columns)

                    calculate_columns = modified_columns + new_columns

                    # Remove duplicated.
                    calculate_columns = list(set(calculate_columns))

                elif not are_actions:
                    # Check if there are columns that have not beend profiler an that are not in the profiler buffer
                    profiler_columns = list(df.meta.get("profile.columns").keys())
                    new_columns = parse_columns(df, columns)

                    calculate_columns = [x for x in new_columns if
                                         not x in profiler_columns or profiler_columns.remove(x)]

            else:
                # Check if all the columns are calculated
                calculate_columns = parse_columns(df, columns)
            return calculate_columns

    return Ext(self)


DataFrame.ext = property(ext)
