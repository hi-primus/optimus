import operator
from abc import abstractmethod, ABC

import humanize
import imgkit
import jinja2
import simplejson as json
from dask import dataframe as dd
from glom import assign
from tabulate import tabulate

from optimus.engines.base.stringclustering import fingerprint_cluster, n_gram_fingerprint_cluster
from optimus.helpers.check import is_notebook
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import BUFFER_SIZE, Actions
from optimus.helpers.functions import absolute_path, reduce_mem_usage, update_dict
from optimus.helpers.json import json_converter, dump_json
from optimus.helpers.output import print_html
from optimus.infer import is_str, is_tuple, is_list
from optimus.profiler.constants import MAX_BUCKETS
from optimus.profiler.templates.html import HEADER, FOOTER
from .columns import BaseColumns
from .meta import Meta
from ...outliers.outliers import Outliers
from ...plots.plots import Plot


class BaseDataFrame(ABC):
    """
    Optimus DataFrame
    """

    def __init__(self, root, data):
        self.data = data
        self.buffer = None
        self.updated = None
        self.root = root
        self.meta = {}

    def _repr_html_(self):
        df = self
        return df.table()

    def __repr__(self):
        df = self
        return df.ascii()

    def __getitem__(self, item):

        if isinstance(item, slice):
            return self.buffer_window("*", item.start, item.stop)
        elif is_str(item) or is_list(item):
            return self.cols.select(item)
        elif isinstance(item, BaseDataFrame):
            return self.root.rows.select(item)

    def __setitem__(self, key, value):
        df = self.cols.assign({key: value})
        self.data = df.data
        self.buffer = df.buffer
        self.updated = df.updated
        self.root = df.root
        self.meta = df.meta

    def new(self, df, meta=None):
        new_df = self.__class__(df)
        if meta is not None:
            new_df.meta = meta
        return new_df

    @staticmethod
    def __operator__(df, dtype):
        if isinstance(df, (BaseDataFrame,)):
            col1 = df.cols.names(0)[0]
            if dtype:
                df = df.cols.cast(col1, dtype).data[col1]
            else:
                df = df.data[col1]
        return df

    @abstractmethod
    def _base_to_dfd(self, pdf, n_partitions):
        pass

    def unary_operation(self, df, opb, dtype=None):
        """
        Helper to process binary operations
        :param df: Left
        :param opb: Operator
        :param dtype: 
        :return:
        """
        df = BaseDataFrame.__operator__(df, dtype)

        return self.new(opb(df).to_frame())

    def operation(self, df1, df2, opb, dtype=None):
        """
        Helper to process binary operations
        :param df1: Left
        :param df2: Right
        :param opb: Operator
        :param dtype:
        :return:
        """
        df1 = BaseDataFrame.__operator__(df1, dtype)
        df2 = BaseDataFrame.__operator__(df2, dtype)

        # Name
        name_left = name_right = ""

        if not isinstance(df1, (int, float, str, dict, list)):
            name_left = df1.name
        if not isinstance(df2, (int, float, str, dict, list)):
            name_right = df2.name
        name = name_left + "_" + name_right

        return self.new(opb(df1, df2).rename(name).to_frame())

    def __invert__(self):
        return self.unary_operation(self, operator.invert)

    def __neg__(self):
        return self.unary_operation(self, operator.neg)

    def __add__(self, df2):
        return self.operation(self, df2, operator.add, "float")

    def __radd__(self, df2):
        return self.operation(df2, self, operator.add, "float")

    def __sub__(self, df2):
        return self.operation(self, df2, operator.sub, "float")

    def __rsub__(self, df2):
        return self.operation(self, df2, operator.sub, "float")

    def __mul__(self, df2):
        return self.operation(self, df2, operator.mul, "float")

    def __rmul__(self, df2):
        return self.operation(df2, self, operator.mul, "float")

    def __truediv__(self, df2):
        return self.operation(self, df2, operator.truediv, "float")

    def __rtruediv__(self, df2):
        return self.operation(df2, self, operator.truediv, "float")

    def __floordiv__(self, df2):
        return self.operation(self, df2, operator.floordiv, "float")

    def __rfloordiv__(self, df2):
        return self.operation(df2, self, operator.floordiv, "float")

    def __mod__(self, df2):
        return self.operation(self, df2, operator.mod, "float")

    def __rmod__(self, df2):
        return self.operation(df2, self, operator.mod, "float")

    def __pow__(self, df2):
        return self.operation(self, df2, operator.pow, "float")

    def __rpow__(self, df2):
        return self.operation(df2, self, operator.pow, "float")

    def __eq__(self, df2):
        return self.operation(self, df2, operator.eq)

    def __gt__(self, df2):
        return self.operation(self, df2, operator.gt, "float")

    def __lt__(self, df2):
        return self.operation(self, df2, operator.lt, "float")

    def __ne__(self, df2):
        return self.operation(self, df2, operator.ne)

    def __ge__(self, df2):
        return self.operation(self, df2, operator.ge, "float")

    def __le__(self, df2):
        return self.operation(self, df2, operator.le, "float")

    def __and__(self, df2):
        return self.operation(self, df2, operator.__and__, "bool")

    def __or__(self, df2):
        return self.operation(self, df2, operator.__or__, "bool")

    def __xor__(self, df2):
        return self.operation(self, df2, operator.__xor__, "bool")

    def _to_values(self):
        """
        Return values from a dataframe in numpy or cupy format. Aimed to be used internally in Machine Learning models
        :return:
        """
        return self.root.data.values

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def rows(self):
        pass

    def cols(self):
        return BaseColumns

    @property
    def plot(self):
        return Plot(self)

    @property
    def outliers(self):
        return Outliers(self)

    @abstractmethod
    def encoding(self):
        pass

    @abstractmethod
    def visualize(self):
        pass

    @staticmethod
    @abstractmethod
    def execute():
        pass

    @staticmethod
    @abstractmethod
    def compute():
        # We will handle all dataframe as if the could compute the result,
        # something that only can be done in dask and in spark triggering and action.
        # With this we expect to abstract the behavior and just use compute() a value from operation
        pass

    def _assign(self, kw_columns):

        dfd = self.root.data
        return dfd.assign(**kw_columns)

    def to_json(self, columns="*"):
        """
        Return a json from a Dataframe
        :return:
        """

        return json.dumps(self.cols.select(columns).to_dict(), ensure_ascii=False, default=json_converter)

    def to_dict(self, orient="records"):
        """
            Return a dict from a Collect result
            [(col_name, row_value),(col_name_1, row_value_2),(col_name_3, row_value_3),(col_name_4, row_value_4)]
            :return:
        """
        return self.root.to_pandas().to_dict(orient)

    @staticmethod
    @abstractmethod
    def sample(n=10, random=False):
        pass

    def columns_sample(self, columns="*"):
        """
        Return a dict of the sample of a Dataframe
        :return:
        """
        df = self

        return {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()],
                "value": df.rows.to_list(columns)}

    @abstractmethod
    def to_pandas(self):
        pass

    def stratified_sample(self, col_name, seed: int = 1):
        """
        Stratified Sampling
        columns_type = parse_columns(df, columns_type.keys())
        :param col_name:
        :param seed:
        :return:
        """
        df = self.data
        # n = min(5, df[col_name].value_counts().min())
        df = df.groupby(col_name).apply(lambda x: x.sample(2))
        # df_.index = df_.index.droplevel(0)
        return df

    def get_buffer(self):
        return self.buffer

    @abstractmethod
    def _buffer_window(self, input_cols, lower_bound, upper_bound):
        pass

    def _reset_buffer(self):
        self.buffer = None

    def reset_buffer(self):
        self._reset_buffer()
        self.meta = Meta.reset(self.meta, "buffer_time")

    def buffer_window(self, columns="*", lower_bound=None, upper_bound=None, n=BUFFER_SIZE):

        df = self

        meta = df.meta
        buffer_time = Meta.get(meta, "buffer_time")
        last_action_time = Meta.get(meta, "last_action_time")

        if buffer_time and last_action_time:
            if buffer_time > last_action_time:
                self.set_buffer(columns, n)

        if lower_bound is None or lower_bound < 0:
            lower_bound = 0

        input_columns = parse_columns(df, columns)
        return self._buffer_window(input_columns, lower_bound, upper_bound)

    def buffer_json(self, columns):
        df = self.buffer
        columns = parse_columns(df, columns)

        return {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()],
                "value": df.rows.to_list(columns)}

    def size(self, deep=True, format=None):
        """
        Get the size of a dask in bytes
        :return:
        """
        df = self.data
        result = df.memory_usage(index=True, deep=deep).sum()
        if format == "human":
            result = humanize.naturalsize(result)

        return result

    def optimize(self, categorical_threshold=50, verbose=False):
        df = self
        return reduce_mem_usage(df, categorical_threshold=categorical_threshold, verbose=verbose)

    def run(self):
        """
        This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
        evaluation approach in processing data: transformation functions are not computed into an action is called.
        Sometimes when transformations are numerous, the computations are very extensive because the high number of
        operations that spark needs to run in order to get the results.

        Other important thing is that Apache Spark save task but not result of dataFrame, so tasks are
        accumulated and the same situation happens.

        :return:
        """
        df = self
        df.cache().count()
        return df

    @staticmethod
    @abstractmethod
    def query(sql_expression):
        raise NotImplementedError

    @staticmethod
    def is_cached(df):
        """

        :return:
        """
        return False if Meta.get(df.meta, "profile") is None else True

    def _cols_to_profile(self, columns):
        """
        Get the columns that needs to be profiled and renames the columns in the metadata
        :return:
        """

        df = self
        actions = Meta.get(df.meta, "transformations.actions")
        has_actions = actions is not None and len(actions) > 0

        profiler_columns = Meta.get(df.meta, "profile.columns")

        new_columns = parse_columns(df, columns)

        if profiler_columns is None:
            calculate_columns = new_columns
        else:
            profiled_columns = list(profiler_columns.keys())
            if not has_actions:
                calculate_columns = [column for column in new_columns if column not in profiled_columns]

            else:
                modified_columns = []
                # Operations need to be processed int the same order that created
                for l in Meta.get(df.meta, "transformations.actions"):
                    dropped_columns = []
                    for action_name, column in l.items():
                        if is_tuple(column):
                            source, target = column

                        elif is_str(column):
                            source = target = column

                        if action_name == Actions.COPY.value and source in profiler_columns:
                            profiler_columns[target] = profiler_columns[source]

                        elif action_name == Actions.RENAME.value and source in profiler_columns:
                            profiler_columns[target] = profiler_columns[source]
                            profiler_columns.pop(source)
                            if source in new_columns:
                                new_columns.pop(source)
                                new_columns.push(target)

                        elif action_name == Actions.DROP.value and source in profiler_columns:
                            profiler_columns.pop(source)
                            dropped_columns.append(source)

                        else:
                            modified_columns.append(source)

                profiled_columns = list(profiler_columns.keys())

                calculate_columns = [column for column in new_columns if column not in profiled_columns]
                modified_columns = [column for column in modified_columns if column in new_columns]
                calculate_columns = list(set(modified_columns + calculate_columns))
                calculate_columns = list(set(calculate_columns) - set(dropped_columns))

        return calculate_columns

    @abstractmethod
    def partitions(self):
        pass

    @staticmethod
    def partitioner():
        raise NotImplementedError

    def repartition(self, n=None, *args, **kwargs):
        df = self.data
        return self.root.new(df, meta=self.root.meta)

    def table_image(self, path, limit=10):
        """
        Output table as image
        :param limit:
        :param path:
        :return:
        """

        css = absolute_path("/css/styles.css")

        imgkit.from_string(self.table_html(limit=limit, full=True), path, css=css)
        print_html("<img src='" + path + "'>")

    def table_html(self, limit=10, columns=None, title=None, full=False, truncate=True, count=True):
        """
        Return a HTML table with the spark cols, data types and values
        :param columns: Columns to be printed
        :param limit: How many rows will be printed
        :param title: Table title
        :param full: Include html header and footer
        :param truncate: Truncate the row information
        :param count:

        :return:
        """

        columns = parse_columns(self, columns)
        if limit is None:
            limit = 10

        df = self
        if limit == "all":
            data = df.cols.select(columns).to_dict()
        else:
            data = df.cols.select(columns).rows.limit(limit).to_dict()
        # Load the Jinja template
        template_loader = jinja2.FileSystemLoader(searchpath=absolute_path("/templates/out"))
        template_env = jinja2.Environment(loader=template_loader, autoescape=True)
        template = template_env.get_template("table.html")

        # Filter only the columns and data type info need it
        dtypes = [(k, v) for k, v in df.cols.dtypes().items()]

        # Remove not selected columns
        final_columns = []
        for i in dtypes:
            for j in columns:
                if i[0] == j:
                    final_columns.append(i)

        # if count is True:

        # else:
        #     count = None
        total_rows = df.rows.approx_count()
        if limit == "all" or total_rows < limit:
            limit = total_rows

        total_rows = humanize.intword(total_rows)
        total_cols = df.cols.count()
        total_partitions = df.partitions()

        df_type = type(df)
        output = template.render(df_type=df_type, cols=final_columns, data=data, limit=limit, total_rows=total_rows,
                                 total_cols=total_cols,
                                 partitions=total_partitions, title=title, truncate=truncate)

        if full is True:
            output = HEADER + output + FOOTER
        return output

    def display(self, limit=10, columns=None, title=None, truncate=True, plain_text=False):
        # TODO: limit, columns, title, truncate
        df = self

        if is_notebook() and not plain_text:
            print_html(df.table(limit, columns, title, truncate))

        else:
            print(df.ascii(limit, columns))

    def print(self, limit=10, columns=None):
        print(self.ascii(limit, columns))

    def table(self, limit=None, columns=None, title=None, truncate=True):
        df = self
        try:
            if is_notebook():
                # TODO: move the html param to the ::: if is_notebook() and engine.output is "html":
                return df.table_html(title=title, limit=limit, columns=columns, truncate=truncate)

        except NameError as e:
            print(e)

        return df.ascii(limit, columns)

    def ascii(self, limit=10, columns=None):
        df = self
        if not columns:
            columns = "*"
        return tabulate(df.rows.limit(limit).cols.select(columns).to_pandas(),
                        headers=[f"""{i}\n({j})""" for i, j in df.cols.dtypes().items()],
                        tablefmt="simple",
                        showindex="never")

    def export(self):
        """
        Helper function to export all the dataframe in text format. Aimed to be used in test functions
        :return:
        """
        df_data = self.to_json()
        df_schema = self.data.dtypes.to_json()

        return f"{df_schema}, {df_data}"

    def show(self, n=10):
        """
        :return:
        """
        return self.data.head(n=n)

    @staticmethod
    @abstractmethod
    def debug():
        pass

    def reset(self):
        # df = self.df
        df = self
        df.meta = {}
        return df

    def calculate_profile(self, columns="*", bins: int = MAX_BUCKETS, flush: bool = False, size=False):
        """
        Returns a new dataframe with the profile data in its added to the meta property
        :param columns:
        :param bins:
        :param flush:
        :param size: get the dataframe size in memory. Use with caution this could be slow for big data frames.
        :return:
        """

        df = self
        meta = self.meta

        previous_columns = Meta.get(meta, "profile.columns")

        if flush is False:
            cols_to_profile = df._cols_to_profile(columns)
        else:
            cols_to_profile = parse_columns(df, columns)

        profiler_data = Meta.get(meta, "profile")

        is_cached = profiler_data is not None

        if profiler_data is None:
            profiler_data = {}
        cols_dtypes = None

        if cols_to_profile or not is_cached or flush is True:
            # Reset profiler metadata
            meta = Meta.set(meta, "profile", {})
            df.meta = meta

            numeric_cols = []
            string_cols = []

            cols_dtypes = {}
            cols_to_infer = [*cols_to_profile]

            for col_name in cols_to_profile:
                _props = Meta.get(df.meta, f"columns_dtypes.{col_name}")

                if _props is not None:
                    cols_dtypes[col_name] = _props
                    cols_to_infer.remove(col_name)

            if cols_to_infer:
                cols_dtypes = {**cols_dtypes, **df.cols.infer_profiler_dtypes(cols_to_infer)}
                cols_dtypes = {col: cols_dtypes[col] for col in cols_to_profile}

            compute = True
            # print("cols_dtypes, compute",cols_dtypes, compute)
            mismatch = df.cols.count_mismatch(cols_dtypes)

            # Get with columns are numerical and does not have mismatch so we can calculate the histogram
            cols = cols_dtypes.items()

            for col_name, properties in cols:
                if properties.get("categorical"):
                    string_cols.append(col_name)
                else:
                    numeric_cols.append(col_name)

            hist = None
            count_uniques = None

            if len(numeric_cols):
                hist = df.cols.hist(numeric_cols, buckets=bins, compute=False)

            freq = None

            if len(string_cols):
                freq = df.cols.frequency(string_cols, n=bins, count_uniques=True, compute=False)

            def merge(_columns, _hist, _freq, _mismatch, _dtypes, _count_uniques):
                _f = {}

                _freq = {} if _freq is None else _freq["frequency"]
                _hist = {} if _hist is None else _hist["hist"]

                for _col_name in _columns:
                    _f[_col_name] = {"stats": _mismatch[_col_name], "dtype": _dtypes[_col_name]}
                    if _col_name in _freq:
                        f = _freq[_col_name]
                        _f[_col_name]["stats"]["frequency"] = f["values"]
                        _f[_col_name]["stats"]["count_uniques"] = f["count_uniques"]

                    elif _col_name in _hist:
                        h = _hist[_col_name]
                        _f[_col_name]["stats"]["hist"] = h
                        # _f[_col_name]["stats"]["count_uniques"] = count_uniques["count_uniques"][_col_name]

                return {"columns": _f}

            # Nulls
            total_count_na = 0

            dtypes = df.cols.dtypes("*")

            if compute is True:
                hist, freq, mismatch = dd.compute(hist, freq, mismatch)

            updated_columns = merge(cols_to_profile, hist, freq, mismatch, dtypes, count_uniques)
            profiler_data = update_dict(profiler_data, updated_columns)

            assign(profiler_data, "name", Meta.get(df.meta, "name"), dict)
            assign(profiler_data, "file_name", Meta.get(df.meta, "file_name"), dict)

            data_set_info = {'cols_count': df.cols.count(),
                             'rows_count': df.rows.count(),
                             }
            if size is True:
                data_set_info.update({'size': df.size(format="human")})

            assign(profiler_data, "summary", data_set_info, dict)
            dtypes_list = list(set(df.cols.dtypes("*").values()))
            assign(profiler_data, "summary.dtypes_list", dtypes_list, dict)
            assign(profiler_data, "summary.total_count_dtypes", len(set([i for i in dtypes.values()])), dict)
            assign(profiler_data, "summary.missing_count", total_count_na, dict)
            assign(profiler_data, "summary.p_missing", round(total_count_na / df.rows.count() * 100, 2))

        all_columns_names = df.cols.names()

        meta = Meta.set(meta, "transformations", value={})

        if not previous_columns:
            previous_columns = {}

        # Order columns
        actual_columns = profiler_data["columns"]
        profiler_data["columns"] = {key: actual_columns[key] for key in all_columns_names if key in actual_columns}
        meta = Meta.set(meta, "profile", profiler_data)

        if cols_dtypes is not None:
            df.meta = meta
            df = df.cols.set_dtype(cols_dtypes, True)
            meta = df.meta

        # Reset Actions
        meta = Meta.reset_actions(meta)
        df.meta = meta

        return df

    def profile(self, columns="*", bins: int = MAX_BUCKETS, output: str = None, flush: bool = False, size=False):
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

        meta = self.meta
        profile = Meta.get(meta, "profile")

        if not profile:
            flush = True

        if columns or flush:
            columns = parse_columns(df, columns) if columns else []
            transformations = Meta.get(meta, "transformations")

            calculate = False

            if flush or len(transformations):
                calculate = True

            else:
                for col in columns:
                    if col not in profile["columns"]:
                        calculate = True

            if calculate:
                df = df[columns].calculate_profile("*", bins, flush, size)
                profile = Meta.get(df.meta, "profile")
                self.meta = df.meta
            profile["columns"] = {key: profile["columns"][key] for key in columns}

        if output == "json":
            profile = dump_json(profile)

        return profile

    def graph(self) -> dict:
        """
        Return a dict the Dask tasks graph
        :return:
        """
        dfd = self.root.data

        return dfd.__dask_graph__().layers

    def get_series(self):
        col1 = self.cols.names(0)[0]
        return self.data[col1]

    def string_clustering(self, columns="*", algorithm="fingerprint"):
        if algorithm == "fingerprint":
            clusters = fingerprint_cluster(self, columns)
        elif algorithm == "n_gram_fingerprint":
            clusters = n_gram_fingerprint_cluster(self, columns)

        return clusters
