import operator
from pprint import pformat

import humanize
import imgkit
import jinja2
import simplejson as json
from glom import assign
from tabulate import tabulate

from optimus.engines.base.columns import *
from optimus.engines.base.constants import BaseConstants
from optimus.engines.base.functions import BaseFunctions
from optimus.engines.base.io.save import *
from optimus.engines.base.mask import Mask
from optimus.engines.base.ml.encoding import BaseEncoding
from optimus.engines.base.ml.models import BaseML
from optimus.engines.base.profile import BaseProfile
from optimus.engines.base.rows import *
from optimus.engines.base.set import BaseSet
from optimus.helpers.check import is_notebook
from optimus.helpers.constants import BUFFER_SIZE
from optimus.helpers.functions import df_dicts_equal, absolute_path, reduce_mem_usage, update_dict
from optimus.helpers.json import json_converter
from optimus.helpers.output import print_html
from optimus.outliers.outliers import Outliers
from optimus.plots.functions import plot_hist, plot_frequency
from optimus.plots.plots import Plot
from optimus.profiler.templates.html import HEADER, FOOTER


class BaseDataFrame(ABC):
    """
    Optimus DataFrame
    """

    def __init__(self, data: 'InternalDataFrameType', op: 'EngineType', label_encoder=None):
        data = self._compatible_data(data)
        self.data = data
        self.buffer = None
        self.updated = None
        self.meta = {}
        self.le = label_encoder
        self.op = op

        # .profile and .set are properties to support docstrings
        self.profile = BaseProfile(self)
        self.set = BaseSet(self)

    @staticmethod
    def _compatible_data(data):
        return data

    def __del__(self):
        del self.data
        del self.le

    @property
    def root(self) -> 'DataFrameType':
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
        self.le = df.le
        self.op = df.op

    def __len__(self):
        return self.rows.count()

    def new(self, dfd, meta=None) -> 'DataFrameType':
        df = self.__class__(dfd, op=self.op)
        if meta is not None:
            df.meta = meta
        import copy
        df.le = copy.deepcopy(self.le)
        return df

    def copy(self) -> 'DataFrameType':
        """
        Return a copy of a dataframe
        """
        df = self.root
        import copy
        return self.root.new(df.data.copy(), meta=copy.deepcopy(df.meta))

    @staticmethod
    def __operator__(df, data_type=None, multiple_columns=False) -> 'DataFrameType':
        if isinstance(df, (BaseDataFrame,)):
            col1 = "*" if multiple_columns else df.cols.names(0)[0]

            if data_type:
                df = df.cols.cast(col1, data_type).data
            else:
                df = df.data

            if not multiple_columns:
                df = df[col1]

        return df

    @abstractmethod
    def _base_to_dfd(self, pdf, n_partitions) -> 'InternalDataFrameType':
        pass

    @abstractmethod
    def to_optimus_pandas(self) -> 'DataFrameType':
        pass

    def unary_operation(self, df, opb, data_type=None) -> 'DataFrameType':
        """
        Helper to process binary operations
        :param df: Dataframe
        :param opb: Operator
        :param data_type: 
        :return:
        """
        df = BaseDataFrame.__operator__(df.get_series(True), data_type, True)

        dfd = opb(df)

        if hasattr(dfd, "to_frame"):
            dfd = dfd.to_frame()

        return self.new(dfd)

    def operation(self, df1, df2, opb, data_type=None) -> 'DataFrameType':
        """
        Helper to process binary operations
        :param df1: Left Dataframe
        :param df2: Right Dataframe
        :param opb: Logical Operator
        :param data_type:
        :return:
        """

        if isinstance(df1, (np.generic,)):
            df1 = np.asscalar(df1)

        if isinstance(df2, (np.generic,)):
            df2 = np.asscalar(df2)

        if is_list(df1):
            df1 = self.op.create.dataframe({"0": df1})
        if is_list(df2):
            df2 = self.op.create.dataframe({"0": df2})

        df1_is_df = isinstance(df1, (BaseDataFrame,))
        df2_is_df = isinstance(df2, (BaseDataFrame,))

        if not df1_is_df or not df2_is_df:
            multiple_columns = True
            if data_type == "auto":
                # finds the type of the value
                not_df = df2 if df1_is_df else df1
                data_type = type(not_df).__name__
                data_type = "float" if data_type == "int" else data_type
            elif data_type in ["float", "int"]:
                df1 = df1 if df1_is_df else float(df1)
                df2 = df2 if df2_is_df else float(df2)
            elif data_type == "str":
                df1 = df1 if df1_is_df else str(df1)
                df2 = df2 if df2_is_df else str(df2)

        else:
            multiple_columns = df1.cols.names() == df2.cols.names()
            if data_type == "auto":
                data_type = None

        def is_spark(df):
            return isinstance(df, BaseDataFrame) and df.op.engine == "spark"

        if (is_spark(df1) or is_spark(df2)) and opb == operator.__or__:
            # We use multiple_columns to extract the series
            multiple_columns = False
            
        df1 = BaseDataFrame.__operator__(df1, data_type, multiple_columns)
        df2 = BaseDataFrame.__operator__(df2, data_type, multiple_columns)

        if multiple_columns:
            dfd = opb(df1, df2)
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

            dfd = opb(df1, df2).rename(name).to_frame()

        return self.new(dfd)

    def __invert__(self) -> 'DataFrameType':
        return self.unary_operation(self, operator.invert)

    def __neg__(self) -> 'DataFrameType':
        return self.unary_operation(self, operator.neg)

    def __add__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.add, "auto")

    def __radd__(self, df2) -> 'DataFrameType':
        return self.operation(df2, self, operator.add, "auto")

    def __sub__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.sub, "float")

    def __rsub__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.sub, "float")

    def __mul__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.mul, "float")

    def __rmul__(self, df2) -> 'DataFrameType':
        return self.operation(df2, self, operator.mul, "float")

    def __truediv__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.truediv, "float")

    def __rtruediv__(self, df2) -> 'DataFrameType':
        return self.operation(df2, self, operator.truediv, "float")

    def __floordiv__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.floordiv, "float")

    def __rfloordiv__(self, df2) -> 'DataFrameType':
        return self.operation(df2, self, operator.floordiv, "float")

    def __mod__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.mod, "float")

    def __rmod__(self, df2) -> 'DataFrameType':
        return self.operation(df2, self, operator.mod, "float")

    def __pow__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.pow, "float")

    def __rpow__(self, df2) -> 'DataFrameType':
        return self.operation(df2, self, operator.pow, "float")

    def __eq__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.eq)

    def __gt__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.gt, "float")

    def __lt__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.lt, "float")

    def __ne__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.ne)

    def __ge__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.ge, "float")

    def __le__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.le, "float")

    def __and__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.__and__, "bool")

    def __or__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.__or__, "bool")

    def __xor__(self, df2) -> 'DataFrameType':
        return self.operation(self, df2, operator.__xor__, "bool")

    def _to_values(self):
        """
        Return values from a dataframe in numpy or cupy format. Aimed to be used internally in Machine Learning models
        :return:
        """
        return self.data.values

    def equals_dataframe(self, df2: 'DataFrameType') -> bool:
        df2 = df2.data
        return self.data.equals(df2)

    def equals(self, df2: 'DataFrameType', decimal=None, assertion=False) -> bool:
        df2_is_dataframe = isinstance(df2, (BaseDataFrame,))

        # checks by column names
        if df2_is_dataframe:
            cols2 = df2.cols.names()
        else:
            cols2 = list(df2.keys())

        cols1 = self.cols.names()

        if cols1 != cols2:
            if assertion:
                raise AssertionError(f"Column names are not equal: {cols1}, {cols2}")
            return False

        if decimal is not None or assertion:
            # checks by each column
            if df2_is_dataframe:
                df2 = df2.to_dict(n="all")
            df1 = self.to_dict(n="all")

            return df_dicts_equal(df1, df2, decimal=decimal, assertion=assertion)

        else:
            # checks by dataframe
            if df2_is_dataframe:
                result = self.equals_dataframe(df2)
                if not result and assertion:
                    raise AssertionError("Dataframes are not equal")
                return result
            else:
                return df_dicts_equal(self.to_dict(n="all"), df2, decimal=decimal, assertion=assertion)

    @property
    @abstractmethod
    def save(self) -> 'BaseSave':
        pass

    @property
    def functions(self) -> 'BaseFunctions':
        return BaseFunctions(self)

    @property
    def mask(self) -> 'Mask':
        return Mask(self)

    @property
    def ml(self) -> 'BaseML':
        return BaseML(self)

    @property
    @abstractmethod
    def rows(self) -> 'BaseRows':
        pass

    @property
    @abstractmethod
    def cols(self) -> 'BaseColumns':
        pass

    @property
    def constants(self) -> 'BaseConstants':
        return BaseConstants()

    @property
    def plot(self) -> 'Plot':
        return Plot(self)

    @property
    def outliers(self) -> 'Outliers':
        return Outliers(self)

    @property
    def encoding(self) -> 'BaseEncoding':
        return BaseEncoding(self)

    def visualize(self):
        raise NotImplementedError(f"\"visualize\" is not available using {type(self).__name__}")

    def execute(self) -> 'DataFrameType':
        return self

    def compute(self) -> 'InternalDataFrameType':
        return self.data

    def _assign(self, kw_columns):

        dfd = self.data
        kw_columns = {str(key): kw_column for key, kw_column in kw_columns.items()}
        return dfd.assign(**kw_columns)

    def to_json(self, cols="*", n="all", orient="list") -> str:
        """

        :param cols:
        :param n:
        :param orient:
        The format of the JSON string:

        ‘split’ : dict like {‘index’ -> [index], ‘columns’ -> [columns], ‘data’ -> [values]}
        ‘records’ : list like [{column -> value}, … , {column -> value}]
        ‘index’ : dict like {index -> {column -> value}}
        ‘columns’ : dict like {column -> {index -> value}}
        ‘values’ : just the values array
        ‘table’ : dict like {‘schema’: {schema}, ‘data’: {data}}

        Describing the data, where data component is like orient='records'.
        :return:
        """

        return json.dumps(self.to_dict(cols, n, orient), ensure_ascii=False, default=json_converter)

    def to_dict(self, cols="*", n: Union[int, str] = 10, orient="list") -> dict:
        """
            Return a dict from a Collect result
            :param cols:
            :param n:
            :param orient:
            :return:
        """
        if n == "all":
            dfd = self.cols.select(cols).to_pandas()
        else:
            dfd = self.buffer_window(cols, 0, n).data

        return dfd.to_dict(orient)

    @staticmethod
    @abstractmethod
    def sample(n=10, random=False):
        pass

    def columns_sample(self, cols="*") -> dict:
        """
        Return a dict of the sample of a Dataframe
        :return:
        """
        df = self

        cols = parse_columns(df, cols)

        return {"columns": [{"title": col_name} for col_name in cols],
                "value": df.rows.to_list(cols)}

    @abstractmethod
    def to_pandas(self):
        pass

    def stratified_sample(self, col_name, seed: int = 1) -> 'DataFrameType':
        """
        Stratified Sampling
        :param col_name:
        :param seed:
        :return:
        """
        raise NotImplementedError(f"\"stratified_sample\" is not available using {type(self).__name__}")

    def get_buffer(self) -> 'DataFrameType':
        """
        Get buffer from the dataframe
        """
        return self.buffer

    @abstractmethod
    def _buffer_window(self, input_cols, lower_bound, upper_bound):
        pass

    def _reset_buffer(self):
        self.buffer = None

    def reset_buffer(self):
        self._reset_buffer()
        self.meta = Meta.reset(self.meta, "buffer_time")

    def buffer_window(self, cols="*", lower_bound=None, upper_bound=None, n=BUFFER_SIZE):
        """
        Get a window from the buffer of the dataframe
        """
        df = self

        meta = df.meta
        buffer_time = Meta.get(meta, "buffer_time")
        last_action_time = Meta.get(meta, "last_action_time")

        if buffer_time and last_action_time:
            if buffer_time > last_action_time:
                self.set_buffer(cols, n)

        if lower_bound is None or lower_bound < 0:
            lower_bound = 0

        input_columns = parse_columns(df, cols)
        return self._buffer_window(input_columns, lower_bound, upper_bound)

    def buffer_json(self, columns):
        df = self.buffer
        columns = parse_columns(df, columns)

        return {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()],
                "value": df.rows.to_list(columns)}

    def size(self, deep=True, format=None):
        """
        Get the size of the dataframe in bytes
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
        if profiler_columns is not None:
            profiler_columns = {
                col_name: value for col_name, value in profiler_columns.items() if value.get("data_type", None)
            }

        new_columns = parse_columns(df, columns) or []

        if profiler_columns is None:
            calculate_columns = new_columns
        else:
            profiled_columns = list(profiler_columns.keys())
            if not has_actions:
                calculate_columns = [
                    column for column in new_columns if column not in profiled_columns]

            else:
                modified_columns = []
                # Operations need to be processed int the same order that created
                for action in Meta.get(df.meta, "transformations.actions"):
                    dropped_columns = []

                    action_name = action.get("name", "action")
                    column = action.get("columns", None)

                    if is_tuple(column):
                        source, target = column

                    else:
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

                calculate_columns = [
                    column for column in new_columns if column not in profiled_columns]
                modified_columns = [
                    column for column in modified_columns if column in new_columns]
                calculate_columns = list(
                    set(modified_columns + calculate_columns))
                calculate_columns = list(
                    set(calculate_columns) - set(dropped_columns))

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

        imgkit.from_string(self.table_html(
            limit=limit, full=True), path, css=css)
        print_html("<img src='" + path + "'>")

    def table_html(self, limit=10, cols=None, title=None, full=False, truncate=True, count=True, highlight=[]):
        """
        Return a HTML table with the spark cols, data types and values
        :param cols: Columns to be printed
        :param limit: How many rows will be printed
        :param title: Table title
        :param full: Include html header and footer
        :param truncate: Truncate the row information
        :param count:
        :param highlight:

        :return:
        """

        cols = parse_columns(self, cols)
        if limit is None:
            limit = 10

        df = self

        total_rows = df.rows.approx_count()

        if limit == "all":
            limit = total_rows
            data = df.cols.select(cols).to_dict(n="all", orient="records")
        else:
            limit = min(limit, total_rows)
            data = df.cols.select(cols).rows.limit(
                limit + 1).to_dict(n="all", orient="records")
        # Load the Jinja template
        template_loader = jinja2.FileSystemLoader(
            searchpath=absolute_path("/templates/out"))
        template_env = jinja2.Environment(
            loader=template_loader, autoescape=True)
        template = template_env.get_template("table.html")

        # Filter only the columns and data type info need it
        data_types = [(k, v) for k, v in df.cols.data_type(tidy=False)["data_type"].items()]

        # Remove not selected columns
        final_columns = []
        for i in data_types:
            for j in cols:
                if i[0] == j:
                    final_columns.append(i)

        total_rows = humanize.intword(total_rows)
        total_cols = df.cols.count()
        total_partitions = df.partitions()
        df_type = type(df)
        highlight = val_to_list(highlight)
        output = template.render(df_type=df_type, cols=final_columns, data=data, limit=limit, total_rows=total_rows,
                                 total_cols=total_cols,
                                 partitions=total_partitions, title=title, truncate=truncate, highlight=highlight)

        if full is True:
            output = HEADER + output + FOOTER
        return output

    def display(self, limit=10, cols=None, title=None, truncate=True, plain_text=False, highlight=None):
        """

        :param limit:
        :param cols: "*", column name or list of column names to be processed.
        :param title:
        :param truncate:
        :param plain_text:
        :param highlight:
        :return:
        """
        if highlight is None:
            highlight = []
        df = self

        if is_notebook() and not plain_text:
            print_html(df.table(limit, cols, title, truncate, highlight))

        else:
            print(df.ascii(limit, cols))

    def print(self, limit=10, cols=None):
        print(self.ascii(limit, cols))

    def table(self, limit=None, cols=None, title=None, truncate=True, highlight=None):
        """
        Print a dataframe in html format
        :param limit: The number of files that will be printed
        :param cols: "*", column name or list of column names to be processed.
        :param title:
        :param truncate:
        :param highlight:
        :return:
        """
        if highlight is None:
            highlight = []
        df = self
        try:
            if is_notebook():
                # TODO: move the html param to the ::: if is_notebook() and engine.output is "html":
                return df.table_html(title=title, limit=limit, cols=cols, truncate=truncate, highlight=highlight)

        except NameError as e:
            print(e)

        return df.ascii(limit, cols)

    def ascii(self, limit=10, cols=None):
        """
        Print a dataframe in ascii format
        :param limit:
        :param cols: "*", column name or list of column names to be processed.
        :return:
        """
        df = self
        if not cols:
            cols = "*"

        limit = min(limit, df.rows.approx_count())
        return tabulate(df.rows.limit(limit + 1).cols.select(cols).to_pandas(),
                        headers=[f"""{i}\n({j})""" for i,
                                                       j in df.cols.data_type(tidy=False)["data_type"].items()],
                        tablefmt="simple",
                        showindex="never") + "\n"

    def export(self, n="all", data_types="inferred"):
        """
        Helper function to export all the dataframe in dict format. Aimed to be used in test functions
        :return:
        """
        df_dict = self.to_dict(n=n)

        if not data_types:
            df_data = pformat(df_dict, sort_dicts=False,
                              width=800, compact=True)
        else:
            if data_types == "internal":
                df_dtypes = self.cols.data_type(tidy=False)["data_type"]
            else:
                df_dtypes = self.cols.infer_type(tidy=False)["infer_type"]
                df_dtypes = {col: df_dtypes[col]["data_type"] for col in df_dtypes}
            df_data = []
            for col_name in df_dict.keys():
                value = pformat((col_name, df_dtypes[col_name]))
                value += ": "
                value += pformat(df_dict[col_name],
                                 sort_dicts=False, width=800, compact=True)
                df_data.append(value)

            df_data = "{" + ", ".join(df_data) + "}"

        return df_data

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

    def calculate_profile(self, cols="*", bins: int = MAX_BUCKETS, flush: bool = False, size=False):
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

        df = self
        meta = self.meta

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
                if properties.get("data_type") in df.constants.NUMERIC_TYPES \
                        and not properties.get("categorical", False):
                    hist_cols.append(col_name)
                else:
                    freq_cols.append(col_name)

            hist = None
            freq = {}
            sliced_freq = {}
            count_uniques = None

            if len(hist_cols):
                _t = time.process_time()
                hist = df.cols.hist(hist_cols, buckets=bins, compute=False)
                profiler_time["hist"] = {
                    "columns": hist_cols, "elapsed_time": time.process_time() - _t}

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

            hist, freq, sliced_freq, mismatch = self.functions.compute(
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

    def graph(self) -> dict:
        raise NotImplementedError(f"Not supported using {type(self).__name__}")

    def get_series(self, multiple_columns: bool = False):
        cols = self.cols.names() if multiple_columns else self.cols.names(0)[0]
        cols = one_list_to_val(cols)
        return self.data[cols]
        # return self.iloc[:, 0]

    def join(self, df_right: 'DataFrameType', how="left", on=None, left_on=None, right_on=None,
             key_middle=False) -> 'DataFrameType':
        """
        Join 2 dataframes using SQL style
        :param df_right: Dataframe used to make the join with.
        :param how {‘left’, ‘right’, ‘outer’, ‘inner’, ‘exclusive’, ‘exclusive left’, ‘exclusive right’}, default ‘left’
        :param on: Key column in both dataframe to make a join.
        :param left_on: Key column on the left used to make a join.
        :param right_on: Key column on the right used to make the join.
        :param key_middle: Order the columns putting the left df columns before the key column and the right df columns

        :return:
        """
        # if not is_(df_right, BaseDataFrame):
        #     RaiseIt.type_error(df_right, ["BaseDataFrame"])

        suffix_left = "_left"
        suffix_right = "_right"

        df_left = self.root

        if on is not None:
            left_on = on
            right_on = on

        if df_left.cols.data_type(left_on) == "category":
            df_left[left_on] = df_left[left_on].cat.as_ordered()

        if df_right.cols.data_type(right_on) == "category":
            df_right[right_on] = df_right[right_on].cat.as_ordered()

        # Join does not work with different data types.
        df_left[left_on] = df_left[left_on].cols.cast("*", "str")
        df_left.data.set_index(left_on)

        df_right[right_on] = df_right[right_on].cols.cast("*", "str")
        df_right.data.set_index(right_on)

        # Used to reorder the output
        left_names = df_left.cols.names()
        # right_names = df_right.cols.names()

        if how in ['exclusive', 'exclusive left', 'exclusive right']:
            _how = 'outer'
            indicator = True
        else:
            _how = how
            indicator = False

        dfd = df_left.data.merge(df_right.data, how=_how, left_on=left_on,
                                 right_on=right_on, suffixes=(suffix_left, suffix_right),
                                 indicator=indicator)

        if how == 'exclusive':
            dfd = dfd[(dfd["_merge"] == "left_only") | (dfd["_merge"] == "right_only")]
        elif how == 'exclusive left':
            dfd = dfd[dfd["_merge"] == "left_only"]
        elif how == 'exclusive right':
            dfd = dfd[dfd["_merge"] == "right_only"]

        if indicator:
            dfd = dfd.drop(["_merge"], axis=1)

        df = self.root.new(dfd)

        # Reorder
        last_column_name = left_names[-1]
        if key_middle is True:
            names = df.cols.names()
            last_column_name = last_column_name if last_column_name in names else last_column_name + suffix_left
            left_on = left_on if left_on in names else left_on + suffix_left
            right_on = right_on if right_on in names else right_on + suffix_right
            if left_on in names:
                df = df.cols.move(left_on, "before", last_column_name)
            if right_on in names:
                df = df.cols.move(right_on, "before", last_column_name)

        return df

    def string_clustering(self, cols="*", algorithm="fingerprint", *args, **kwargs):
        from optimus.engines.base.stringclustering import string_clustering
        return string_clustering(self, cols, algorithm, *args, **kwargs)
        # return clusters

    def agg(self, aggregations: dict, groupby=None, output="dict", tidy=True):
        """
        :param aggregations: Dictionary or list of tuples with the form [("col", "agg")]
        :param groupby: None, list of columns names or a single column name to group the aggregations.
        :param output:{‘dict’, ‘dataframe’}, default ‘dict’: Output type.
        :param tidy: The result format. If 'True' it will return a value if you 'False' will return the column name a value.
        process a column or column name and value if not. If False it will return the functions name, the column name

        """

        df = self
        dfd = df.data

        if is_dict(aggregations):
            aggregations = aggregations.items()

        if groupby:
            groupby = parse_columns(df, groupby)
            aggregations = {column: val_to_list(aggregations_set) for column, aggregations_set in aggregations}
            dfd = dfd.groupby(groupby).agg(aggregations)

            dfd.columns = ['_'.join(col).strip() for col in dfd.columns.values]

            if output == "dict":
                result = dfd.to_dict()

            elif output == "dataframe":
                dfd.columns = [str(c) for c in dfd.columns]
                result = self.new(dfd.reset_index())

        else:
            result = {}

            for column, aggregations_set in aggregations:
                aggregations_set = val_to_list(aggregations_set)
                for aggregation in aggregations_set:
                    result[column + "_" + aggregation] = getattr(
                        df.cols, aggregation)(column, tidy=True)

            if output == "dataframe":
                result = self.op.create.dataframe({k: [v] for k, v in result.items()})

        return convert_numpy(format_dict(result, tidy=tidy))

    def report(self, df, cols="*", buckets=MAX_BUCKETS, infer=False, relative_error=RELATIVE_ERROR,
               approx_count=True,
               mismatch=None, advanced_stats=True):
        """
        Return dataframe statistical information in HTML Format
        :param df: Dataframe to be analyzed
        :param cols: Columns to be analyzed
        :param buckets: Number of buckets calculated to print the histogram
        :param infer: infer data type
        :param relative_error: Relative Error for quantile discretizer calculation
        :param approx_count: Use approx_count_distinct or countDistinct
        :param mismatch:
        :param advanced_stats:
        :return:
        """

        cols = parse_columns(df, cols)
        output = self.dataset(df, cols, buckets, infer, relative_error, approx_count, format="dict",
                              mismatch=mismatch, advanced_stats=advanced_stats)

        # Load jinja
        template_loader = jinja2.FileSystemLoader(
            searchpath=absolute_path("/profiler/templates/out"))
        template_env = jinja2.Environment(
            loader=template_loader, autoescape=True)

        # Render template
        # Create the profiler info header
        html = ""
        general_template = template_env.get_template("general_info.html")
        html = html + general_template.render(data=output)

        template = template_env.get_template("one_column.html")
        # Create every column stats
        for col_name in cols:
            hist_pic = None
            freq_pic = None

            col = output["columns"][col_name]
            if "hist" in col["stats"]:
                hist_dict = col["stats"]["hist"]

                if col["column_data_type"] == "date":
                    hist_year = plot_hist(
                        {col_name: hist_dict["years"]}, "base64", "years")
                    hist_month = plot_hist(
                        {col_name: hist_dict["months"]}, "base64", "months")
                    hist_weekday = plot_hist(
                        {col_name: hist_dict["weekdays"]}, "base64", "weekdays")
                    hist_hour = plot_hist(
                        {col_name: hist_dict["hours"]}, "base64", "hours")
                    hist_minute = plot_hist(
                        {col_name: hist_dict["minutes"]}, "base64", "minutes")
                    hist_pic = {"hist_years": hist_year, "hist_months": hist_month, "hist_weekdays": hist_weekday,
                                "hist_hours": hist_hour, "hist_minutes": hist_minute}

                elif col["column_data_type"] == "int" or col["column_data_type"] == "string" or col[
                    "column_data_type"] == "float":
                    hist = plot_hist({col_name: hist_dict}, output="base64")
                    hist_pic = {"hist_numeric_string": hist}
            if "frequency" in col:
                freq_pic = plot_frequency(
                    {col_name: col["frequency"]}, output="base64")

            html = html + \
                   template.render(data=col, freq_pic=freq_pic, hist_pic=hist_pic)

        # Save in case we want to output to a html file
        # self.html = html + df.table_html(10)
        self.html = html

        # Display HTML
        print_html(self.html)

        # JSON
        # Save in case we want to output to a json file
        self.json = output

        return self
