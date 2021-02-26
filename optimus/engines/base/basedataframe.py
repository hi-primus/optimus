import operator
import time
from abc import abstractmethod, ABC
from collections import OrderedDict

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
from optimus.helpers.constants import PROFILER_NUMERIC_DTYPES, BUFFER_SIZE
from optimus.helpers.functions import absolute_path, reduce_mem_usage, update_dict
from optimus.helpers.json import json_converter, dump_json
from optimus.helpers.output import print_html
from optimus.infer import is_list_of_str, is_dict
from optimus.profiler.constants import MAX_BUCKETS
from optimus.profiler.templates.html import HEADER, FOOTER
from .columns import BaseColumns
from .meta import Meta


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
        # try:
        return df.table()
        # except Exception:
        #     pass

    def __repr__(self):
        df = self
        # try:
        return df.ascii()
        # except Exception:
        #     pass

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.buffer_window("*", item.start, item.stop)
        return self.cols.select(item)

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
        col1 = df.cols.names(0)[0]
        if dtype:
            df = df.cols.cast(col1, dtype).data[col1]
        else:
            df = df.data[col1]
        return df

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

        return self.new(opb(df1, df2).to_frame())

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
        return self.unary_operation(self, operator.eq)

    def __gt__(self, df2):
        return self.operation(self, df2, operator.gt, "float")

    def __lt__(self, df2):
        return self.operation(self, df2, operator.lt, "float")

    def __ne__(self, df2):
        return self.operation(self, df2, operator.ne)

    def __invert__(self):
        return self.unary_operation(self, operator.__invert__)

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

    def cols(self):
        return BaseColumns

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

    @staticmethod
    def _assign(series):
        pass

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
    def _create_buffer_df(self, input_cols, n):
        pass

    @abstractmethod
    def _buffer_window(self, input_cols, lower_bound, upper_bound):
        pass

    def set_buffer(self, columns="*", n=BUFFER_SIZE):
        df = self
        input_cols = parse_columns(self, columns)

        df_length = self.rows.count()

        if n > df_length:
            n = df_length

        self.buffer = self._create_buffer_df(input_cols, n)
        Meta.set(self.meta, "buffer_time", int(time.time()))

    def buffer_window(self, columns=None, lower_bound=None, upper_bound=None, n=BUFFER_SIZE):

        df = self

        meta = df.meta
        buffer_time = Meta.get(meta, "buffer_time")
        last_action_time = Meta.get(meta, "last_action_time")

        if buffer_time and last_action_time:
            if buffer_time > last_action_time:
                df.set_buffer(columns, n)

        df_buffer = df.get_buffer()

        if df_buffer is None:
            df.set_buffer(columns, n)
            df_buffer = df.get_buffer()

        df_length = df_buffer.rows.count()

        if lower_bound is None:
            lower_bound = 0

        if lower_bound < 0:
            lower_bound = 0

        if upper_bound is None:
            upper_bound = df_length

        if upper_bound > df_length:
            upper_bound = df_length

        if lower_bound >= df_length:
            diff = upper_bound - lower_bound
            lower_bound = df_length - diff
            upper_bound = df_length

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
        return False if df.meta.get("profile.profiler_dtype") is None else True

    def to_delayed(self):
        return self.data.to_delayed()

    def calculate_cols_to_profile(self, df, columns):
        """
        Get the columns that needs to be profiled.
        :return:
        """
        # Metadata
        # If not empty the profiler already run.
        # So process the dataframe's metadata to get which columns need to be profiled
        df = self
        actions = df.meta.get("transformations.actions")
        are_actions = actions is not None and len(actions) > 0

        # print("are actions", are_actions)

        def get_columns(action):
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
        if self.is_cached(df):
            if are_actions:

                def get_columns_by_action(action):
                    """
                    Get a list of columns which have been applied and specific action.
                    :param action:
                    :return:
                    """
                    # modified = []

                    col = get_columns(action)
                    # Check if was renamed
                    renamed_columns = get_renamed_columns(col)
                    if len(renamed_columns) == 0:
                        _result = col
                    else:
                        _result = renamed_columns

                    return _result

                def get_renamed_columns(_col_names):
                    """
                    Get a list of columns and return the renamed version.
                    :param _col_names:
                    :return:
                    """
                    _renamed_columns = []
                    _rename = get_columns("rename")

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

                            rename = get_columns("rename")
                            if rename:
                                for l in rename:
                                    for k, v in l.items():
                                        profiler_columns[v] = profiler_columns.pop(k)
                                        profiler_columns[v]["name"] = v

                        # Drop Keys
                        elif action_name == "drop":

                            for col_names in get_columns_by_action(action_name):
                                # print("action_name, col_names", action_name, col_names)
                                # print("profiler_columns",profiler_columns)
                                profiler_columns.pop(col_names)
                        else:
                            modified_columns = modified_columns + get_columns_by_action(action_name)
                # Actions applied to current columns

                # Remove duplicated
                temp_calculate_columns = list(set(modified_columns + new_columns))
                calculate_columns = []
                # If after some  action the columns is dropped we need to remove it from the modified columns
                for col_name in get_columns_by_action("drop"):
                    for temp_calculate_column in temp_calculate_columns:
                        if temp_calculate_column != col_name and (temp_calculate_column not in new_columns):
                            calculate_columns.append(temp_calculate_column)

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

    # def set_name(self, value=None):
    #     """
    #     Create a temp view for a data frame also used in the json output profiling
    #     :param value:
    #     :return:
    #     """
    #     df = self.df
    #     df._name = value
    #     # if not is_str(value):
    #     #     RaiseIt.type_error(value, ["string"])
    #
    #     # if len(value) == 0:
    #     #     RaiseIt.value_error(value, ["> 0"])
    #
    #     # self.createOrReplaceTempView(value)

    @staticmethod
    @abstractmethod
    def partitions():
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

        # print(data)
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
        return self.head(n=n)

    @staticmethod
    @abstractmethod
    def debug():
        pass

    def head(self, columns="*", n=10):
        """

        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)
        return df.data[columns].head(n)

    def reset(self):
        # df = self.df
        df = self
        df.meta = {}
        return df

    def profile(self, columns="*", bins: int = MAX_BUCKETS, output: str = None, flush: bool = False, size=False):
        """
        Return profiler info
        :param columns:
        :param bins:
        :param output:
        :param flush:
        :param size: get the dataframe size in memory. Use with caution this could be slow for big data frames.
        :return:
        """

        df = self
        meta = self.meta

        if flush is False:
            cols_to_profile = df.calculate_cols_to_profile(df, columns)
        else:
            cols_to_profile = parse_columns(df, columns)

        profiler_data = Meta.get(meta, "profile")
        if profiler_data is None:
            profiler_data = {}
        cols_and_inferred_dtype = None

        if cols_to_profile or not self.is_cached(df) or flush is True:
            numeric_cols = []
            string_cols = []
            cols_and_inferred_dtype = df.cols.infer_profiler_dtypes(cols_to_profile)

            compute = True
            # print("cols_and_inferred_dtype, compute",cols_and_inferred_dtype, compute)
            mismatch = df.cols.count_mismatch(cols_and_inferred_dtype)

            # Get with columns are numerical and does not have mismatch so we can calculate the histogram
            for col_name, x in cols_and_inferred_dtype.items():
                if x["dtype"] in PROFILER_NUMERIC_DTYPES and mismatch[col_name]["mismatch"] == 0:
                    numeric_cols.append(col_name)
                else:
                    string_cols.append(col_name)

            hist = None
            freq_uniques = None

            if len(numeric_cols):
                hist = df.cols.hist(numeric_cols, buckets=bins, compute=compute)
                freq_uniques = df.cols.count_uniques(numeric_cols, estimate=False, compute=compute, tidy=False)

            freq = None
            if len(string_cols):
                freq = df.cols.frequency(string_cols, n=bins, count_uniques=True, compute=compute)

            # print(numeric_cols, string_cols)

            def merge(_columns, _hist, _freq, _mismatch, _dtypes, _freq_uniques):
                _f = {}

                for _col_name in _columns:
                    _f[_col_name] = {"stats": _mismatch[_col_name], "dtype": _dtypes[_col_name]}

                if _hist is not None:
                    for _col_name, h in _hist["hist"].items():
                        _f[_col_name]["stats"]["hist"] = h
                        # print("freq_uniques",freq_uniques)
                        _f[_col_name]["stats"]["count_uniques"] = freq_uniques["count_uniques"][_col_name]

                if _freq is not None:
                    for _col_name, f in _freq["frequency"].items():
                        _f[_col_name]["stats"]["frequency"] = f["values"]
                        _f[_col_name]["stats"]["count_uniques"] = f["count_uniques"]

                return {"columns": _f}

            # Nulls
            total_count_na = 0

            dtypes = df.cols.dtypes("*")

            if compute is True:
                hist, freq, mismatch, freq_uniques = dd.compute(hist, freq, mismatch, freq_uniques)

            updated_columns = merge(cols_to_profile, hist, freq, mismatch, dtypes, freq_uniques)
            profiler_data = update_dict(profiler_data, updated_columns)

            assign(profiler_data, "name", df.meta.get("name"), dict)
            assign(profiler_data, "file_name", df.meta.get("file_name"), dict)

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

        actual_columns = profiler_data["columns"]
        # Order columns
        columns = parse_columns(df, columns)
        profiler_data["columns"] = dict(OrderedDict(
            {_cols_name: actual_columns[_cols_name] for _cols_name in columns if
             _cols_name in list(actual_columns.keys())}))

        df.meta = {}
        meta = Meta.columns(meta, df.cols.names())

        meta = Meta.set(meta, "transformations", value={})

        meta = Meta.set(meta, "profile", profiler_data)

        if cols_and_inferred_dtype is not None:
            df.cols.set_profiler_dtypes(cols_and_inferred_dtype)

        # Reset Actions
        meta = Meta.reset_actions(meta)
        df.meta = meta
        if output == "json":
            profiler_data = dump_json(profiler_data)
        return profiler_data

    def get_series(self):
        col1 = self.cols.names(0)[0]
        return self.data[col1]

    def string_clustering(self, columns="*", algorithm="fingerprint"):
        if algorithm == "fingerprint":
            clusters = fingerprint_cluster(self, columns)
        elif algorithm == "n_gram_fingerprint":
            clusters = n_gram_fingerprint_cluster(self, columns)

        return clusters
