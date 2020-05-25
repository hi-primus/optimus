from abc import abstractmethod, ABC

import humanize
import imgkit
import jinja2
import math
import numpy as np
import simplejson as json

from optimus.bumblebee import Comm
from optimus.engines.base.contants import SAMPLE_NUMBER
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import RELATIVE_ERROR, BUFFER_SIZE, Actions
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import absolute_path, collect_as_dict, reduce_mem_usage
from optimus.helpers.functions_spark import traverse
from optimus.helpers.json import json_converter
from optimus.helpers.output import print_html
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_of_str, is_dict
from optimus.profiler.profiler import Profiler
from optimus.profiler.templates.html import HEADER, FOOTER


class BaseExt(ABC):
    # _name = None
    _name = None

    def __init__(self, df):
        self.df = df
        # self.buffer_a = None

    @staticmethod
    @abstractmethod
    def cache():
        pass

    def to_json(self, columns="*", format="bumblebee"):
        """
        Return a json from a Spark Dataframe
        :return:
        """
        df = self.df
        if format == "bumblebee":
            columns = parse_columns(df, columns)
            result = {"sample": {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()],
                                 "value": df.rows.to_list(columns)}}
        else:
            result = json.dumps(df.ext.to_dict(df), ensure_ascii=False, default=json_converter)

        return result

    def to_dict(self):
        """
        Return a dict from a Collect result
        [(col_name, row_value),(col_name_1, row_value_2),(col_name_3, row_value_3),(col_name_4, row_value_4)]
        :return:
        """
        df = self.df
        return collect_as_dict(df)

    def export(self):
        """
        Helper function to export all the spark in text format. Aimed to be used in test functions
        :return:
        """
        df = self.df
        dict_result = {}

        value = df.collect()
        schema = []
        for col_name in df.cols.names():

            data_type = df.cols.schema_dtype(col_name)
            if isinstance(data_type, np.array):
                data_type = "ArrayType(" + str(data_type.elementType) + "()," + str(data_type.containsNull) + ")"
            else:
                data_type = str(data_type) + "()"

            nullable = df.schema[col_name].nullable

            schema.append(
                "('{name}', {dataType}, {nullable})".format(name=col_name, dataType=data_type, nullable=nullable))
        schema = ",".join(schema)
        schema = "[" + schema + "]"

        # if there is only an element in the dict just return the value
        if len(dict_result) == 1:
            dict_result = next(iter(dict_result.values()))
        else:
            dict_result = [tuple(v.asDict().values()) for v in value]

        def func(path, _value):
            try:
                if math.isnan(_value):
                    r = None
                else:
                    r = _value
            except TypeError:
                r = _value
            return r

        dict_result = traverse(dict_result, None, func)

        return "{schema}, {dict_result}".format(schema=schema, dict_result=dict_result)

    @staticmethod
    @abstractmethod
    def sample(n=10, random=False):
        pass

    def stratified_sample(self, col_name, seed: int = 1):
        """
        Stratified Sampling
        :param col_name:
        :param seed:
        :return:
        """
        df = self.df
        n = min(5, df[col_name].value_counts().min())
        df = df.groupby(col_name).apply(lambda x: x.sample(2))
        # df_.index = df_.index.droplevel(0)
        return df

    @staticmethod
    @abstractmethod
    def pivot(index, column, values):
        """
        Return reshaped DataFrame organized by given index / column values.
        :param index: Column to use to make new frame's index.
        :param column: Column to use to make new frame's columns.
        :param values: Column(s) to use for populating new frame's values.
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
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

        pass

    # def set_buffer(self, columns, n=BUFFER_SIZE):
    #     pass

    def get_buffer(self):
        # return self.df._buffer.values.tolist()
        df = self.df
        return df._buffer

    def buffer_window(self, columns=None, lower_bound=None, upper_bound=None):
        df = self.df._buffer

        df_length = len(df)
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
            # RaiseIt.value_error(df_length, str(df_length - 1))

        input_columns = parse_columns(df, columns)
        return df[input_columns][lower_bound: upper_bound]

    def buffer_json(self, columns):
        df = self.df._buffer
        columns = parse_columns(df, columns)

        return {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()],
                "value": df.rows.to_list(columns)}

    def size(self, deep=True, format=None):
        """
        Get the size of a dask in bytes
        :return:
        """
        df = self.df
        result = df.memory_usage(index=True, deep=deep).sum()
        if format == "human":
            result = humanize.naturalsize(result)

        return result

    def optimize(self, categorical_threshold=50, verbose=False):
        df = self.df
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
        df = self.df
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
        if BaseExt.is_cached(df):
            if are_actions:

                def get_columns_by_action(action):
                    """
                    Get a list of columns which have been applied and specific action.
                    :param action:
                    :return:
                    """
                    modified = []

                    col = get_columns(action)
                    # Check if was renamed
                    if len(get_renamed_columns(col)) == 0:
                        _result = col
                    else:
                        _result = get_renamed_columns(col)

                    # Unnest return a list inside a list
                    if action == Actions.UNNEST.value:
                        _result = _result[0]
                    modified = modified + _result
                    return modified

                def get_renamed_columns(_col_names):
                    """
                    Get a list of columns and return the renamed version.
                    :param _col_names:
                    :return:
                    """
                    _renamed_columns = []

                    _rename = get_columns("rename")

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

                            rename = get_columns("rename")
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
                            modified_columns = modified_columns + (get_columns_by_action(action_name))

                # Actions applied to current columns
                # print("modified_columns", modified_columns)
                # print("new_columns", new_columns)

                calculate_columns = modified_columns + new_columns

                # Remove duplicated
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

    def set_name(self, value=None):
        """
        Create a temp view for a data frame also used in the json output profiling
        :param value:
        :return:
        """
        df = self.df
        df.ext._name = value
        # if not is_str(value):
        #     RaiseIt.type_error(value, ["string"])

        # if len(value) == 0:
        #     RaiseIt.value_error(value, ["> 0"])

        # self.createOrReplaceTempView(value)

    def get_name(self):
        """
        Get dataframe name
        :return:
        """
        df = self.df
        return df.ext._name

    @staticmethod
    @abstractmethod
    def partitions():
        pass

    @staticmethod
    def partitioner():
        raise NotImplementedError

    def repartition(self, n=None, *args, **kwargs):
        df = self.df
        df = df.repartition(npartitions=n, *args, **kwargs)
        return df

    @staticmethod
    def table_image(path, limit=10):
        """
        Output table as image
        :param limit:
        :param path:
        :return:
        """

        css = absolute_path("/css/styles.css")

        imgkit.from_string(BaseExt.table_html(limit=limit, full=True), path, css=css)
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

        df = self.df

        columns = parse_columns(df, columns)
        if limit is None:
            limit = 10

        if limit == "all":
            data = df.cols.select(columns).ext.to_dict()
        else:
            data = collect_as_dict(df.cols.select(columns).rows.limit(limit))

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

        if count is True:
            total_rows = df.rows.approx_count()
        else:
            count = None

        if limit == "all":
            limit = total_rows
        elif total_rows < limit:
            limit = total_rows

        total_rows = humanize.intword(total_rows)
        total_cols = df.cols.count()
        total_partitions = df.ext.partitions()

        # print(data)
        df_type = type(df)
        output = template.render(df_type=df_type, cols=final_columns, data=data, limit=limit, total_rows=total_rows,
                                 total_cols=total_cols,
                                 partitions=total_partitions, title=title, truncate=truncate)

        if full is True:
            output = HEADER + output + FOOTER
        return output

    def display(self, limit=None, columns=None, title=None, truncate=True):
        # TODO: limit, columns, title, truncate

        self.table(limit, columns, title, truncate)

    def table(self, limit=None, columns=None, title=None, truncate=True):
        df = self.df
        try:
            if __IPYTHON__:
                # TODO: move the html param to the ::: if __IPYTHON__ and engine.output is "html":
                result = df.ext.table_html(title=title, limit=limit, columns=columns, truncate=truncate)
                print_html(result)
            else:
                df.ext.show()
        except NameError:

            df.show()

    @staticmethod
    @abstractmethod
    def show():
        pass

    @staticmethod
    @abstractmethod
    def debug():
        pass

    # @staticmethod
    # @abstractmethod
    # def create_id(column="id"):
    #     """
    #     Create a unique id for every row.
    #     :param column: Columns to be processed
    #     :return:
    #     """
    #
    #     pass

    def send(self, name: str = None, infer: bool = False, mismatch=None, stats: bool = True,
             advanced_stats: bool = True,
             output: str = "http", sample=SAMPLE_NUMBER):
        """
        Profile and send the data to the queue
        :param name: Specified a name for the view/spark
        :param infer:
        :param mismatch:
        :param stats:
        :param advanced_stats: Process advance stats
        :param output: 'json' or 'dict'
        :param sample: Number of data sample returned
        :return:
        """
        df = self.df
        if name is not None:
            df.ext.set_name(name)

        message = Profiler.instance.dataset(df, columns="*", buckets=35, infer=infer, relative_error=RELATIVE_ERROR,
                                            approx_count=True,
                                            sample=sample,
                                            stats=stats,
                                            format="json",
                                            advanced_stats=advanced_stats,
                                            mismatch=mismatch
                                            )
        if Comm.instance:
            return Comm.instance.send(message, output=output)
        else:
            raise Exception("Comm is not initialized. Please use comm=True param like Optimus(comm=True)")

    def reset(self):
        df = self.df
        df = df.meta.set(None, {})
        return df
