import base64
from functools import reduce
from io import BytesIO

import dateutil.parser
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from fastnumbers import isint, isfloat
from numpy import array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

# Helpers
from optimus.helpers.checkit import is_tuple, is_, is_one_element, is_list_of_tuples, is_column
from optimus.helpers.convert import one_list_to_val
from optimus.helpers.functions import get_spark_dtypes_object, infer, is_pyarrow_installed, random_int
from optimus.helpers.logger import logger
from optimus.helpers.parser import parse_python_dtypes
from optimus.helpers.raiseit import RaiseIt
from optimus.spark import Spark


def abstract_udf(col, func, func_return_type=None, attrs=None, func_type=None, verbose=False):
    """
    Abstract User defined functions. This is a helper function to create udf, pandas udf or a Column Exp
    :param col: Column to created or transformed
    :param func: Function to be applied to the data
    :param attrs: If required attributes to be passed to the function
    :param func_return_type: Required by UDF and Pandas UDF.
    :param func_type: pandas_udf or udf. The function is going to try to use pandas_udf if func_type is not defined
    :param verbose: print additional info
    :return: A function, UDF or Pandas UDF
    """

    if func_return_type is None:
        func_type = "column_exp"
    # By default is going to try to use pandas UDF
    elif func_type is None and is_pyarrow_installed() is True:
        func_type = "pandas_udf"

    types = ["column_exp", "udf", "pandas_udf"]
    if func_type not in types:
        RaiseIt.value_error(func_type, types)

    # It handle if func param is a plain expression or a function returning and expression
    def func_col_exp(col_name, attr):
        return func

    if is_column(func):
        _func = func_col_exp
    else:
        _func = func

    logger.print(
        "Using '{func_type}' to process column '{column}' with function {func_name}".format(func_type=func_type,
                                                                                            column=col,
                                                                                            func_name=_func.__name__))
    df_func = func_factory(func_type, func_return_type)

    return df_func(attrs, _func)(col)


def func_factory(func_type=None, func_return_type=None):
    """
    Return column express, udf or pandas udf function.
    :param func_type: Type of function udf or pandas udf
    :param func_return_type:
    :return:
    """

    # if func_return_type is not None:
    func_return_type = get_spark_dtypes_object(func_return_type)

    def pandas_udf_func(attr=None, func=None):
        # TODO: Get the column type, so is not necessary to pass the return type as param.

        # Apply the function over the whole series
        def apply_to_series(val, attr):
            if attr is None:
                attr = (None,)
            else:
                attr = (attr,)

            return val.apply(func, args=attr)

        def to_serie(value):
            return apply_to_series(value, attr)

        return F.pandas_udf(to_serie, func_return_type)

    def udf_func(attr, func):
        return F.udf(lambda value: func(value, attr), func_return_type)

    def expression_func(attr, func):
        def inner(col_name):
            return func(col_name, attr)

        return inner

    if func_type is "pandas_udf":
        return pandas_udf_func

    elif func_type is "udf":
        return udf_func

    elif func_type is "column_exp":
        return expression_func


def filter_row_by_data_type_audf(col_name, data_type):
    """
    Filter a column using a Spark data type as reference
    :param col_name:
    :param data_type:
    :return:
    """

    data_type = parse_python_dtypes(data_type)
    return abstract_udf(col_name, filter_row_by_data_type, "boolean", data_type)


def append(dfs, like="columns"):
    """
    Concat multiple dataframes as columns or rows
    :param dfs:
    :param like: The way dataframes is going to be concat. like columns or rows
    :return:
    """

    # FIX: Because monotonically_increasing_id can create different
    # sequence for different dataframes the result could be wrong.

    if like == "columns":
        temp_dfs = []
        col_temp_name = "id_" + random_int()
        for df in dfs:
            temp_dfs.append(df.withColumn(col_temp_name, F.monotonically_increasing_id()))

        def _append(df1, df2):
            return df1.join(df2, col_temp_name, "outer")

        df_result = reduce(_append, temp_dfs).drop(col_temp_name)

    elif like == "rows":
        df_result = reduce(DataFrame.union, dfs)
    else:
        RaiseIt.value_error(like, ["columns", "rows"])

    return df_result


def output_image(path):
    """
    Output a png file
    :param path: Matplotlib figure
    :return: Base64 encode image
    """

    plt.savefig(path, format='png')
    plt.close()


def output_base64(fig):
    """
    Output a matplotlib as base64 encode
    :param fig: Matplotlib figure
    :return: Base64 encode image
    """
    fig_file = BytesIO()
    plt.savefig(fig_file, format='png')
    # rewind to beginning of file
    fig_file.seek(0)

    fig_png = base64.b64encode(fig_file.getvalue())
    plt.close(fig)

    return fig_png.decode('utf8')


def ellipsis(data, length=20):
    """
    Add a "..." if a string y greater than a specific length
    :param data:
    :param length: length taking into account to cut the string
    :return:
    """
    data = str(data)
    return (data[:length] + '..') if len(data) > length else data


def plot_scatterplot(column_data=None, output=None, path=None):
    """
    Boxplot
    :param column_data: column data in json format
    :param output: image or base64
    :param path:
    :return:
    """

    fig = plt.figure(figsize=(12, 5))
    plt.scatter(column_data["x"]["data"], column_data["y"]["data"], s=column_data["s"], alpha=0.5)
    plt.xlabel(column_data["x"]["name"])
    plt.ylabel(column_data["y"]["name"])

    # Tweak spacing to prevent clipping of tick-labels
    # plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)

    # Save as base64
    if output is "base64":
        return output_base64(fig)
    elif output is "image":
        return output_image(path)


def plot_boxplot(column_data=None, output=None, path=None):
    """
    Boxplot
    :param column_data: column data in json format
    :param output: image or base64
    :param path:
    :return:
    """
    for col_name, stats in column_data.items():
        fig, axes = plt.subplots(1, 1)

        bp = axes.bxp(stats, patch_artist=True)

        axes.set_title(col_name)
        plt.figure(figsize=(12, 5))

        # 'fliers', 'means', 'medians', 'caps'
        for element in ['boxes', 'whiskers']:
            plt.setp(bp[element], color='#1f77b4')

        for patch in bp['boxes']:
            patch.set(facecolor='white')

            # Tweak spacing to prevent clipping of tick-labels

        # Save as base64
        if output is "base64":
            return output_base64(fig)
        elif output is "image":
            return output_image(path)
        else:
            plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)

def plot_freq(column_data=None, output=None, path=None):
    """
    Frequency plot
    :param column_data: column data in json format
    :param output: image or base64
    :param path:
    :return:
    """
    for col_name, data in column_data.items():

        # Transform Optimus' format to matplotlib's format
        x = []
        h = []

        for d in data:
            x.append(ellipsis(d["value"]))
            h.append(d["count"])

        # Plot
        fig = plt.figure(figsize=(12, 5))

        # Need to to this to plot string labels on x
        x_i = range(len(x))
        plt.bar(x_i, h)
        plt.xticks(x_i, x)

        plt.title("Frequency '" + col_name + "'")

        plt.xticks(rotation=45, ha="right")

        # Tweak spacing to prevent clipping of tick-labels
        plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)

        # Save as base64
        if output is "base64":
            return output_base64(fig)
        elif output is "image":
            return output_image(path)


def plot_missing_values(column_data=None, output=None):
    """
    Plot missing values
    :param column_data:
    :param output: image o base64
    :return:
    """
    values = []
    columns = []
    labels = []
    for col_name, data in column_data["data"].items():
        values.append(data["missing"])
        columns.append(col_name)
        labels.append(data["%"])

    # Plot
    fig = plt.figure(figsize=(12, 5))
    plt.bar(columns, values)
    plt.xticks(columns, columns)

    # Highest limit
    highest = column_data["count"]
    plt.ylim(0, 1.05 * highest)
    plt.title("Missing Values")
    i = 0
    for label, val in zip(labels, values):
        plt.text(x=i - 0.5, y=val + (highest * 0.05), s="{}({})".format(val, label))
        i = i + 1

    plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)

    # Save as base64
    if output is "base64":
        return output_base64(fig)


def plot_hist(column_data=None, output=None, sub_title="", path=None):
    """
    Plot a histogram
    obj = {"col_name":[{'lower': -87.36666870117188, 'upper': -70.51333465576172, 'value': 0},
    {'lower': -70.51333465576172, 'upper': -53.66000061035157, 'value': 22094},
    {'lower': -53.66000061035157, 'upper': -36.80666656494141, 'value': 2},
    ...
    ]}
    :param column_data: column data in json format
    :param output: image or base64
    :param sub_title: plot subtitle
    :param path:
    :return: plot, image or base64
    """

    for col_name, data in column_data.items():
        bins = []
        for d in data:
            bins.append(d['lower'])

        last = data[len(data) - 1]["upper"]
        bins.append(last)

        # Transform hist Optimus format to matplot lib format
        hist = []
        for d in data:
            if d is not None:
                hist.append(d["count"])

        bins = array(bins)
        center = (bins[:-1] + bins[1:]) / 2
        width = 0.9 * (bins[1] - bins[0])

        # Plot
        fig = plt.figure(figsize=(12, 5))
        plt.bar(center, hist, width=width)
        plt.title("Histogram '" + col_name + "' " + sub_title)

        plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.3)

        # Save as base64
        if output is "base64":
            return output_base64(fig)
        elif output is "image":
            return output_image(path)


def plot_correlation(column_data, output=None, path=None):
    """
    Plot a correlation plot
    :param column_data:
    :return:
    """
    return sns.heatmap(column_data, mask=np.zeros_like(column_data, dtype=np.bool),
                       cmap=sns.diverging_palette(220, 10, as_cmap=True))


def filter_row_by_data_type(col_name, data_type=None, get_type=False):
    """
    A Pandas UDF function that returns bool if the value match with the data_type param passed to the function.
    Also can return the data type
    :param col_name: Column to be process
    :param data_type: The data_type to be compared with
    :param get_type: Value to be returned as string or boolean
    :return: True or False
    """
    from ast import literal_eval

    if data_type is not None:
        data_type = parse_python_dtypes(data_type)

    def pandas_udf_func(v):
        def str_to_boolean(value):
            """
            Check if a str can be converted to boolean
            :param value:
            :return:
            """
            value = value.lower()
            if value == "true" or value == "false":
                return True

        def str_to_date(value):
            try:
                dateutil.parser.parse(value)
                return True
            except ValueError:
                pass

        def str_to_array(value):
            """
            Check if value can be parsed to a tuple or and array.
            Because Spark can handle tuples we will try to transform tuples to arrays
            :param value:
            :return:
            """
            try:
                if isinstance(literal_eval((value.encode('ascii', 'ignore')).decode("utf-8")), (list, tuple)):
                    return True
            except (ValueError, SyntaxError,):
                pass

        def func(value):
            """
            Check if a value can be casted to a specific
            :param value: value to be checked
            :return:
            """
            if isinstance(value, bool):
                _data_type = "bool"
            # _data_type = data_type
            elif isint(value):  # Check if value is integer
                _data_type = "int"
            elif isfloat(value):
                _data_type = "float"
            # if string we try to parse it to int, float or bool
            elif isinstance(value, str):
                if str_to_boolean(value):
                    _data_type = "bool"
                elif str_to_date(value):
                    _data_type = "date"
                elif str_to_array(value):
                    _data_type = "array"
                else:
                    _data_type = "string"
            else:
                _data_type = "null"

            if get_type is False:
                if _data_type == data_type:
                    return True
                else:
                    return False
            else:
                return _data_type

        return v.apply(func)

    if get_type is True:
        return_data_type = "string"
    else:
        return_data_type = "boolean"

    col_name = one_list_to_val(col_name)
    return F.pandas_udf(pandas_udf_func, return_data_type)(col_name)


class Create:
    @staticmethod
    def data_frame(cols=None, rows=None, infer_schema=True, pdf=None):
        """
        Helper to create a Spark dataframe:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples with the same number and types that cols
        :param infer_schema: Try to infer the schema data type.
        :param pdf: a pandas dataframe
        :return: Dataframe
        """
        if is_(pdf, pd.DataFrame):
            result = Spark.instance.spark.createDataFrame(pdf)
        else:

            specs = []
            # Process the rows
            if not is_list_of_tuples(rows):
                rows = [(i,) for i in rows]

            # Process the columns
            for c, r in zip(cols, rows[0]):
                # Get columns name

                if is_one_element(c):
                    col_name = c

                    if infer_schema is True:
                        var_type = infer(r)
                    else:
                        var_type = StringType()
                    nullable = True

                elif is_tuple(c):

                    # Get columns data type
                    col_name = c[0]
                    var_type = get_spark_dtypes_object(c[1])

                    count = len(c)
                    if count == 2:
                        nullable = True
                    elif count == 3:
                        nullable = c[2]

                # If tuple has not the third param with put it to true to accepts Null in columns
                specs.append([col_name, var_type, nullable])

            struct_fields = list(map(lambda x: StructField(*x), specs))

            result = Spark.instance.spark.createDataFrame(rows, StructType(struct_fields))

        return result

    df = data_frame
