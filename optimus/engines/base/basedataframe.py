import operator
import time
from abc import abstractmethod, ABC

import humanize
import imgkit
import jinja2
import simplejson as json
from dask import dataframe as dd
from glom import assign
from tabulate import tabulate

from optimus.engines.base.stringclustering import string_clustering
from optimus.helpers.check import is_notebook
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import BUFFER_SIZE, Actions, ProfilerDataTypes, RELATIVE_ERROR
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.functions import absolute_path, reduce_mem_usage, update_dict
from optimus.helpers.json import json_converter
from optimus.helpers.output import print_html
from optimus.infer import is_str, is_tuple, is_list
from optimus.profiler.constants import MAX_BUCKETS
from optimus.profiler.templates.html import HEADER, FOOTER
from .columns import BaseColumns
from .meta import Meta
from .profile import BaseProfile
from ...outliers.outliers import Outliers
from ...plots.functions import plot_hist, plot_frequency
from ...plots.plots import Plot


class BaseDataFrame(ABC):
    """
    Optimus DataFrame
    """

    def __init__(self, data):
        self.data = data
        self.buffer = None
        self.updated = None
        self.meta = {}

    def __del__(self):
        del self.data

    @property
    def root(self):
        return self

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
            return self.rows.select(item)

    def __setitem__(self, key, value):
        df = self.cols.assign({key: value})
        self.data = df.data
        self.buffer = df.buffer
        self.updated = df.updated
        self.meta = df.meta

    def __len__(self):
        return self.rows.count()

    def new(self, dfd, meta=None):
        df = self.__class__(dfd)
        if meta is not None:
            df.meta = meta
        return df

    @staticmethod
    def __operator__(df, dtype, multiple_columns=False):
        if isinstance(df, (BaseDataFrame,)):
            col1 = "*" if multiple_columns else df.cols.names(0)[0]

            if dtype:
                df = df.cols.cast(col1, dtype).data
            else:
                df = df.data

            if not multiple_columns:
                df = df[col1]

        return df

    @abstractmethod
    def _base_to_dfd(self, pdf, n_partitions):
        pass

    @abstractmethod
    def to_optimus_pandas(self):
        pass

    def unary_operation(self, df, opb, dtype=None):
        """
        Helper to process binary operations
        :param df: Left
        :param opb: Operator
        :param dtype: 
        :return:
        """
        df = BaseDataFrame.__operator__(df, dtype, True)

        return self.new(opb(df))

    def operation(self, df1, df2, opb, dtype=None):
        """
        Helper to process binary operations
        :param df1: Left
        :param df2: Right
        :param opb: Operator
        :param dtype:
        :return:
        """
        if (not isinstance(df1, (BaseDataFrame,)) or not isinstance(df2, (BaseDataFrame,))):
            multiple_columns = True
        else:
            multiple_columns = df1.cols.names() == df2.cols.names()
        df1 = BaseDataFrame.__operator__(df1, dtype, multiple_columns)
        df2 = BaseDataFrame.__operator__(df2, dtype, multiple_columns)

        if multiple_columns:
            df = self.new(opb(df1, df2))

        else:

            name_left = name_right = ""

            if not isinstance(df1, (int, float, str, dict, list)):
                name_left = getattr(df1, "name", 0)

            if not isinstance(df2, (int, float, str, dict, list)):
                name_right = getattr(df2, "name", 0)

            if name_left and name_right:
                name = (name_left + "_" + name_right) if name_left != name_right else name_left
            else:
                name = name_left if name_left else name_right

            df = self.new(opb(df1, df2).rename(name).to_frame())

        return df

    def __invert__(self):
        return self.unary_operation(self, operator.invert)

    def __neg__(self):
        return self.unary_operation(self, operator.neg)

    def __add__(self, df2):
        return self.operation(self, df2, operator.add)

    def __radd__(self, df2):
        return self.operation(df2, self, operator.add)

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
        return self.data.values

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def rows(self):
        pass

    def cols(self):
        return BaseColumns

    @property
    def profile(self):
        return BaseProfile(self)

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

        dfd = self.data
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
        return self.to_pandas().to_dict(orient)

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
        return self.new(df, meta=self.meta)

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

        total_rows = df.rows.approx_count()

        if limit == "all":
            limit = total_rows
            data = df.cols.select(columns).to_dict()
        else:
            limit = min(limit, total_rows)
            data = df.cols.select(columns).rows.limit(limit + 1).to_dict()
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
        limit = min(limit, df.rows.approx_count())
        return tabulate(df.rows.limit(limit + 1).cols.select(columns).to_pandas(),
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
        _t = time.process_time()
        profiler_time = {"hist": {}, "frequency": {}, "count_mismatch": {}}

        df = self
        meta = self.meta

        if flush is False:
            cols_to_profile = df._cols_to_profile(columns) or []
        else:
            cols_to_profile = parse_columns(df, columns) or []

        profiler_data = Meta.get(meta, "profile")

        is_cached = profiler_data is not None

        if profiler_data is None:
            profiler_data = {}
        cols_dtypes = None

        profiler_time["beginning"] = {"elapsed_time": time.process_time() - _t}
        if cols_to_profile or not is_cached or flush is True:
            # Reset profiler metadata
            meta = Meta.set(meta, "profile", {})
            df.meta = meta

            hist_cols = []
            freq_cols = []

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

            _t = time.process_time()
            mismatch = df.cols.count_mismatch(cols_dtypes)
            profiler_time["count_mismatch"] = {"columns": cols_dtypes, "elapsed_time": time.process_time() - _t}

            # Get with columns are numerical and does not have mismatch so we can calculate the histogram
            cols = cols_dtypes.items()
            for col_name, properties in cols:
                if properties.get("categorical") is True \
                        or properties.get("dtype") == ProfilerDataTypes.EMAIL.value \
                        or properties.get("dtype") == ProfilerDataTypes.URL.value \
                        or properties.get("dtype") == ProfilerDataTypes.OBJECT.value:
                    freq_cols.append(col_name)
                else:
                    hist_cols.append(col_name)

            hist = None
            freq = {}
            sliced_freq = {}
            count_uniques = None

            if len(hist_cols):
                _t = time.process_time()
                hist = df.cols.hist(hist_cols, buckets=bins, compute=False)
                profiler_time["hist"] = {"columns": hist_cols, "elapsed_time": time.process_time() - _t}

            if len(freq_cols):
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
                    freq = df.cols.frequency(non_sliced_cols, n=bins, count_uniques=True, compute=False)

                if len(sliced_cols) > 0:
                    # print("sliced_cols", sliced_cols)
                    sliced_freq = df.cols.slice(sliced_cols, 0, 50).cols.frequency(sliced_cols, n=bins, count_uniques=True,
                                                                         compute=False)

                profiler_time["frequency"] = {"columns": freq_cols, "elapsed_time": time.process_time() - _t}

            def merge(_columns, _hist, _freq, _mismatch, _dtypes, _count_uniques):
                _c = {}
                
                _hist = {} if _hist is None else _hist["hist"]
                _freq = {} if _freq is None else _freq["frequency"]

                for _col_name in _columns:
                    _c[_col_name] = {"stats": _mismatch[_col_name], "dtype": _dtypes[_col_name]}
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

            dtypes = df.cols.dtypes("*")

            hist, freq, sliced_freq, mismatch = dd.compute(hist, freq, sliced_freq, mismatch)

            freq = {**freq, **sliced_freq}

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

        # _t = time.process_time()

        all_columns_names = df.cols.names()

        meta = Meta.set(meta, "transformations", value={})

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
        profiler_time["end"] = {"elapsed_time": time.process_time() - _t}
        # print(profiler_time)
        return df

    def graph(self) -> dict:
        """
        Return a dict the Dask tasks graph
        :return:
        """
        dfd = self.data

        return dfd.__dask_graph__().layers

    def get_series(self):
        col1 = self.cols.names(0)[0]
        return self.data[col1]

    def string_clustering(self, columns="*", algorithm="fingerprint", *args, **kwargs):
        return string_clustering(self, columns, algorithm, *args, **kwargs)
        # return clusters

    def agg(self, aggregations: dict, groupby=None, output="dict"):

        df = self
        dfd = df.data

        if groupby:
            groupby = parse_columns(df, groupby)

            for column, aggregations_set in aggregations.items():
                aggregations[column] = val_to_list(aggregations_set)

            dfd = dfd.groupby(groupby).agg(aggregations)

            dfd.columns = ['_'.join(col).strip() for col in dfd.columns.values]

            if output == "dict":
                result = dfd.to_dict()

            elif output == "dataframe":
                result = self.new(dfd.reset_index())

        else:
            result = {}

            for column, aggregations_set in aggregations.items():
                aggregations_set = val_to_list(aggregations_set)
                for aggregation in aggregations_set:
                    result[column + "_" + aggregation] = getattr(df.cols, aggregation)(column, tidy=True)

            if output == "dataframe":
                result = self.new(result)

        return result

    def report(self, df, columns="*", buckets=MAX_BUCKETS, infer=False, relative_error=RELATIVE_ERROR,
               approx_count=True,
               mismatch=None, advanced_stats=True):
        """
        Return dataframe statistical information in HTML Format
        :param df: Dataframe to be analyzed
        :param columns: Columns to be analyzed
        :param buckets: Number of buckets calculated to print the histogram
        :param infer: infer data type
        :param relative_error: Relative Error for quantile discretizer calculation
        :param approx_count: Use approx_count_distinct or countDistinct
        :param mismatch:
        :param advanced_stats:
        :return:
        """

        columns = parse_columns(df, columns)
        output = self.dataset(df, columns, buckets, infer, relative_error, approx_count, format="dict",
                              mismatch=mismatch, advanced_stats=advanced_stats)

        # Load jinja
        template_loader = jinja2.FileSystemLoader(searchpath=absolute_path("/profiler/templates/out"))
        template_env = jinja2.Environment(loader=template_loader, autoescape=True)

        # Render template
        # Create the profiler info header
        html = ""
        general_template = template_env.get_template("general_info.html")
        html = html + general_template.render(data=output)

        template = template_env.get_template("one_column.html")
        # Create every column stats
        for col_name in columns:
            hist_pic = None
            freq_pic = None

            col = output["columns"][col_name]
            if "hist" in col["stats"]:
                hist_dict = col["stats"]["hist"]

                if col["column_dtype"] == "date":
                    hist_year = plot_hist({col_name: hist_dict["years"]}, "base64", "years")
                    hist_month = plot_hist({col_name: hist_dict["months"]}, "base64", "months")
                    hist_weekday = plot_hist({col_name: hist_dict["weekdays"]}, "base64", "weekdays")
                    hist_hour = plot_hist({col_name: hist_dict["hours"]}, "base64", "hours")
                    hist_minute = plot_hist({col_name: hist_dict["minutes"]}, "base64", "minutes")
                    hist_pic = {"hist_years": hist_year, "hist_months": hist_month, "hist_weekdays": hist_weekday,
                                "hist_hours": hist_hour, "hist_minutes": hist_minute}

                elif col["column_dtype"] == "int" or col["column_dtype"] == "string" or col[
                    "column_dtype"] == "decimal":
                    hist = plot_hist({col_name: hist_dict}, output="base64")
                    hist_pic = {"hist_numeric_string": hist}
            if "frequency" in col:
                freq_pic = plot_frequency({col_name: col["frequency"]}, output="base64")

            html = html + template.render(data=col, freq_pic=freq_pic, hist_pic=hist_pic)

        # Save in case we want to output to a html file
        # self.html = html + df.table_html(10)
        self.html = html

        # Display HTML
        print_html(self.html)

        # JSON
        # Save in case we want to output to a json file
        self.json = output

        return self
