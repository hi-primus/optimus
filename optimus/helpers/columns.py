import math
import re
from typing import Union

from ordered_set import OrderedSet

# from optimus.helpers.check import is_spark_dataframe, is_pandas_dataframe, is_dask_dataframe, is_cudf_dataframe

from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.logger import logger
from optimus.helpers.parser import parse_data_types
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_list_value, is_tuple, is_list_of_str, is_list_of_list, is_list_of_tuples, is_str, is_list, \
    is_int, is_list_of_int


def replace_columns_special_characters(df, replace_by="_"):
    """
    Remove special character from Spark column name
    :param df: Spark Dataframe
    :param replace_by: character or string that will replace the matched character
    :return:
    """

    def replace_multiple_characters(string, to_be_replaced, replace_by):
        """
        Replace multiple single characters in s string
        :param string:
        :param to_be_replaced: Character to be replaced
        :param replace_by: character or string that will replace the matched character
        :return:
        """
        # Iterate over the strings to be replaced
        for elem in to_be_replaced:
            # Check if string is in the main string
            if elem in string:
                # Replace the string
                string = string.replace(elem, replace_by)
        return string

    for col_name in df.cols.names():
        df = df.cols.rename(col_name, replace_multiple_characters(col_name, ["."], replace_by))
    return df


def escape_columns(columns):
    """
    Add a backtick to a columns name to prevent the dot in name problem
    :param columns:
    :return:
    """

    escaped_columns = []
    if is_list_value(columns):
        for col in columns:
            # Check if the column is already escaped
            if col[0] != "`" and col[len(col) - 1] != "`":
                escaped_columns.append("`" + col + "`")
            else:
                escaped_columns.append(col)
    else:
        # Check if the column is already escaped
        if columns[0] != "`" and columns[len(columns) - 1] != "`":
            escaped_columns = "`" + columns + "`"
        else:
            escaped_columns.append(columns)

    return escaped_columns


def get_output_cols(cols, output_cols=None, merge=False, auto_increment=False):
    """
    Construct output columns taking the input columns.
    If it receive a list of input columns and on output column the function will append the output_col name to the input cols list

    If
    cols = ['col1'] to output_cols = None
    cols = ['col1'] to output_cols = ['col1']


    If merge is True

    For 1 column
    cols = ['col1'] to output_cols = 'new'
    Result:
    cols = ['col1'] to output_cols = ['col1_new']

    For multiple columns
    cols = ['col1', 'col2'...'col3'] to output_cols = 'new'
    Result:
    cols = ['col1', 'col2',...'col3'] to output_cols = ['col1_new', 'col2_new',...'col3_new']

    else merge is False
    For 1 column
    cols = ['col1'] to output_cols = 'new'
    cols = ['col1'] to output_cols = ['new']

    :param cols:
    :param output_cols:
    :param merge:
    :param auto_increment:
    :return:
    """

    # if is_list(cols) and is_list(output_cols):
    #     if len(cols) != len(output_cols):
    #         RaiseIt.length_error(cols, output_cols)

    if output_cols is None:
        output_cols = val_to_list(cols)
    elif callable(output_cols):
        output_cols = list(map(output_cols, cols))
    else:
        output_cols = val_to_list(output_cols)

        # if auto_increment is not None:

    if auto_increment is True:
        # cols = cols * auto_increment
        # output_cols = val_to_list(output_cols)
        # print("LEAN @", len(cols)/auto_increment)
        # r = int(len(cols) / auto_increment)
        r = list(range(auto_increment)) * 2
        output_cols = [col_name + "_" + str(i) for i, col_name in zip(r, cols)]
    elif merge is True:
        output_cols = val_to_list(output_cols)
        output_cols = list([name_col(input_col, output_cols) for input_col in cols])

    return output_cols


def parse_columns(df, cols_args, is_regex=None, filter_by_column_types=None, accepts_missing_cols=False, invert=False):
    """
    Return a list of columns and check that columns exists in the spark
    Accept '*' as parameter in which case return a list of all columns in the spark.
    Also accept a regex.
    If a list of tuples return to list. The first element is the columns name the others element are params.
    This params can be used to create custom transformation functions. You can find and example in cols().cast()
    :param df: Dataframe in which the columns are going to be checked
    :param cols_args: Accepts * as param to return all the string columns in the spark
    :param is_regex: Use True is cols_args is a regex
    :param filter_by_column_types: A data type for which a columns list is going be filtered
    :param accepts_missing_cols: if true not check if column exist in the spark
    :param invert: Invert the final selection. For example if you want to select not integers

    :return: A list of columns string names
    """
    
    # if columns value is * get all dataframes columns

    df_columns = df.cols._names()

    if is_regex is True:
        r = re.compile(cols_args)
        cols = list(filter(r.match, df_columns))

    elif cols_args == "*" or cols_args is None:
        cols = df_columns

    elif is_int(cols_args):
        cols = val_to_list(df_columns[cols_args])

    elif is_list_of_int(cols_args):
        cols = list(df_columns[i] for i in cols_args)

    elif is_tuple(cols_args) or is_list_of_tuples(cols_args):
        # In case we have a list of tuples we use the first element of the tuple is taken as the column name
        # and the rest as params. We can use the param in a custom function as follow
        # def func(attrs): attrs return (1,2) and (3,4)
        #   return attrs[0] + 1
        # df.cols().apply([('col_1',1,2),('cols_2', 3 ,4)], func)

        # Verify if we have a list with tuples

        cols_args = val_to_list(cols_args)
        # Extract a specific position in the tuple
        cols = [(i[0:1][0]) for i in cols_args]
        attrs = [(i[1:]) for i in cols_args]

    else:
        # if not a list convert to list
        cols = val_to_list(cols_args)
        # Get col name from index
        cols = [c if is_str(c) else df_columns[c] for c in cols]

    # Check for missing columns
    if accepts_missing_cols is False:
        check_for_missing_columns(df, cols)

    # Filter by column data type
    filter_by_column_types = val_to_list(filter_by_column_types)
    if is_list_of_list(filter_by_column_types):
        filter_by_column_types = [
            item for sublist in filter_by_column_types for item in sublist]

    columns_residual = None

    # If necessary filter the columns by data type
    if filter_by_column_types:
        # Get columns for every data type
        columns_filtered = names_by_data_types(
            df, filter_by_column_types)

        # Intersect the columns filtered per data type from the whole spark with the columns passed to the function
        final_columns = list(OrderedSet(cols).intersection(columns_filtered))

        # This columns match filtered data type
        columns_residual = list(OrderedSet(cols) - OrderedSet(columns_filtered))

    else:
        final_columns = cols

    cols_params = []
    if invert:
        final_columns = list(OrderedSet(df_columns) - OrderedSet(final_columns))

    cols_params = final_columns

    if columns_residual:
        logger.print("%s %s %s", ",".join(escape_columns(columns_residual)),
                     "column(s) was not processed because is/are not",
                     ",".join(filter_by_column_types))

    # if because of filtering we got 0 columns return None
    if len(cols_params) == 0:
        cols_params = None
        logger.print("Outputting 0 columns after filtering. Is this expected?")

    return cols_params


def prepare_columns(df, cols: Union[str, list], output_cols: Union[str, list] = None, is_regex=None,
                    filter_by_column_types=None, accepts_missing_cols=False, invert: bool = False, default=None,
                    cols_dict=None, auto_increment=False, args=None):
    """
    One input columns- > Same output column. lower(), upper()
    One input column -> One output column. copy()
    One input column -> Multiple output column. unnest()
    Multiple input columns -> One output column. nest()
    Multiple input columns -> Multiple output columns. lower(), upper()

    For multiple output we can pass a string, a list o columns or just enumerate de output column.
    See get_output_cols() for more info

    Accepts Return an iterator with input and output columns
    :param df: dataframe against to check that the input columns are valid
    :param cols: intput columns names
    :param output_cols: output columns names
    :param is_regex: input columns is a regex
    :param filter_by_column_types: filter column selection by data type
    :param accepts_missing_cols: dont check the input columns exist
    :param invert: Invert selection
    :param default: Default column name if output_cols is not provider
    :param cols_dict: In case you have a input output dictionary already defined. {incol:outcol1 , incol2:outcol2}
    :param auto_increment:
    :param merge:
    :return:
    """
    if cols_dict:
        result = zip(*cols_dict)
    else:
        cols = parse_columns(df, cols, is_regex, filter_by_column_types,
                                   accepts_missing_cols, invert)
        merge = False
        if output_cols is None and default is not None:
            output_cols = default
            merge = True

        elif auto_increment is not False:
            cols = cols * auto_increment

        if output_cols is not None and (len(cols) != len(val_to_list(output_cols))):
            merge = True

        output_cols = get_output_cols(cols, output_cols, merge=merge, auto_increment=auto_increment)

        if args is None:
            result = zip(cols, output_cols)
        else:
            args = val_to_list(args)
            if len(args) == 1:
                args = args * len(cols)

            result = zip(cols, output_cols, args)
    return result


def check_column_numbers(columns, number=0):
    """
    Check if the columns number match number expected
    :param columns:
    :param number: Number of columns to check
    :return:
    """
    if columns is None:
        RaiseIt.value_error(columns, ["str", "list"],
                            extra_text="Maybe the columns selected do not match a specified datatype filter.")

    if isinstance(columns, zip):
        columns = list(columns)

    count = list(columns)

    if number == "*":
        if not len(columns) >= 1:
            RaiseIt.value_error(len(columns), ["1 or greater"])
    elif number == ">1":
        if not len(columns) > 1:
            RaiseIt.value_error(len(columns), ["more than 1"])
    elif len(columns) != number:
        RaiseIt.value_error(count, "{} columns, {} needed".format(number, columns))
    # elif isinstance(columns,zip):


def validate_columns_names(df, col_names: Union[str, list], index=0):
    """
    Check if a string or list of string are valid spark columns
    :param df: Dataframe to be analyzed
    :param col_names: columns names to be checked
    :param index:
    :return:
    """

    columns = val_to_list(col_names)

    if is_list_of_tuples(columns):
        columns = [c[index] for c in columns]

    # Remove duplicates in the list

    if is_list_of_str(columns):
        columns = OrderedSet(columns)

    check_for_missing_columns(df, columns)

    return True


def check_for_missing_columns(df, col_names):
    """
    Check if the columns you want to select exits in the dataframe
    :param df: Dataframe to be checked
    :param col_names: cols names to
    :return:
    """
    _col_names = df.cols._names()
    missing_columns = list(OrderedSet(col_names) - OrderedSet(_col_names))

    if len(missing_columns) > 0:
        RaiseIt.value_error(missing_columns, _col_names)
    return False


def names_by_data_types(df, data_type):
    """
    Return column names filtered by the column data type
    :param df: Dataframe which columns are going to be filtered
    :param data_type: Datatype used to filter the column.
    :type data_type: str or list
    :return:
    """
    parsed_data_type = parse_data_types(df, data_type)
    data_type = val_to_list(data_type)
    parsed_data_type = val_to_list(parsed_data_type)
    # Filter columns by data type
    result = []
    for col_name in df.cols.names():
        found_parsed_data_type = df.cols.schema_data_type(col_name)
        found_data_type = df.cols.data_type(col_name)
        if any([dt in found_parsed_data_type for dt in parsed_data_type])\
        or any([dt in found_data_type for dt in data_type]):
            result.append(col_name)
    return result


def name_col(col_names: str, append: str = None) -> str:
    """
    Whenever you want to name and output use this function. This ensure that we use and Standard when naming
    :param col_names: Column name
    :param append: string to be appended
    :return:
    """
    separator = "_"

    col_names = val_to_list(col_names)
    if len(col_names) > 1 and append is None:
        output_col = ('_'.join(str(elem) for elem in col_names))
    elif len(col_names) > 1:
        output_col = ('_'.join(str(elem) for elem in col_names))[:10] + separator
    else:
        output_col = one_list_to_val(col_names)

    if append is not None:
        append = separator + str(one_list_to_val(append))
    else:
        append = ""
    return output_col + append


def prepare_columns_arguments(cols, *args):
    """
    Prepare arguments for multiple columns.
    """

    cols = val_to_list(cols)
    args = list(args)
    
    for i, arg in enumerate(args):
        arg = val_to_list(arg, allow_none=True, convert_tuple=True)
        if len(cols) > len(arg):
            arg *= math.floor(len(cols) / len(arg))

        args[i] = arg[0:len(cols)]

    return one_list_to_val(args)
