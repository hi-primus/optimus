from dask.dataframe.core import DataFrame

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
        def profile(columns, bins=10, output=None):
            """

            :param columns:
            :param bins:
            :param output:
            :return:
            """

            df = self
            df_length = len(df)

            columns = parse_columns(df, columns)
            result = {"sample": {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()]}}

            df = self
            result["columns"] = {}
            numeric_cols = df.cols.names(filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
            string_cols = df.cols.names(filter_by_column_dtypes=df.constants.STRING_TYPES)

            hist = df.cols.hist(numeric_cols, buckets=bins)
            freq = df.cols.frequency(string_cols, n=bins)
            from dask import delayed

            @delayed
            def merge(_columns, _hist, _freq, _rows_count, output):
                _h = {}
                r = {}
                # for col_name in columns:
                for col_name, h in _hist.items():
                    r[col_name] = {}
                    r[col_name]["stats"] = {}
                    r[col_name]["missing"] = 1
                    r[col_name]["mismatch"] = 1
                    r[col_name]["null"] = 0

                    r[col_name]["stats"]["hist"] = h["hist"]

                for col_name, h in _freq.items():
                    r[col_name] = {}
                    r[col_name]["stats"] = {}
                    r[col_name]["missing"] = 1
                    r[col_name]["mismatch"] = 1
                    r[col_name]["null"] = 0

                    r[col_name]["stats"]["frequency"] = h["frequency"]

                if output == "json":
                    r = dump_json(r)

                return {"columns": r, "stats": {"rows_count": _rows_count}}

            return merge(columns, hist, freq, df_length, output).compute()
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
        def size(deep=False):
            """
            Get the size of a dask in bytes
            :return:
            """
            df = self
            result = df.memory_usage(index=True, deep=deep).sum().compute()
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


def is_cached(self):
    """

    :return:
    """
    return len(self.output_columns) > 0


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
    if self.is_cached():
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
            profiler_columns = self.output_columns["columns"]
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
