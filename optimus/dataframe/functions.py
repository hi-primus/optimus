# Used in decorators. This convenience func preserves name and docstring
from functools import wraps

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import unicodedata
from pyspark.sql.functions import col, udf, trim, lit, format_number, months_between, date_format, unix_timestamp, \
    current_date, abs as mag

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

from optimus.spark import *
from optimus.assertion_helpers import *

# You can use string, str or String as param
TYPES = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int',
         'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}

# Instead StringType() just use string
DICT_TYPES = {'string': StringType(), 'int': IntegerType(), 'float': FloatType(), 'double': DoubleType()}

# Alias
Dataframe = create_df

def query(self):
    """
    Select row depending of a query
    :return:
    """
    # https://stackoverflow.com/questions/11869910/pandas-filter-rows-of-dataframe-with-operator-chaining
    print("hola")


# Reference https://medium.com/@mgarod/dynamically-add-a-method-to-a-class-in-python-c49204b85bd6
# Decorator to attach a custom functions to a class

def add_method(cls):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        setattr(cls, func.__name__, wrapper)
        # Note we are not binding func, but wrapper which accepts self but does exactly the same as func
        return func  # returning func means func can still be used normally

    return decorator


@add_method(DataFrame)
def lookup(self, columns, look_up_key=None, replace_by=None):
    """
    This method search a list of strings specified in `list_str` argument among rows
            in column dataFrame and replace them for `str_to_replace`.
    :param columns: Column name, this variable must be string dataType.
    :param look_up_key: List of strings to be replaced
    :param replace_by: string that going to replace all others present in list_str argument
    :return:
    """

    # Check if columns argument a string datatype:
    self._assert_type_str(columns, "column")

    # Asserting columns is string or list:
    assert isinstance(replace_by, (str, dict)), "Error: str_to_replace argument must be a string or a dict"

    if isinstance(replace_by, dict):
        assert (replace_by != {}), "Error, str_to_replace must be a string or a non empty python dictionary"
        assert (
                look_up_key is None), "Error, If a python dictionary if specified, list_str argument must be None: list_str=None"

    # Asserting columns is string or list:
    assert isinstance(look_up_key, list) and look_up_key != [] or (
            look_up_key is None), "Error: Column argument must be a non empty list"

    if isinstance(replace_by, str):
        assert look_up_key is not None, "Error: list_str cannot be None if str_to_replace is a String, please you need to specify \
                 the list_str string"

    # Filters all string columns in dataFrame
    valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

    if isinstance(columns, str):
        columns = [columns]

    # Check if columns to be process are in dataframe
    self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

    # Asserting if selected column datatype and search and changeTo parameters are the same:
    col_not_valids = (set(columns).difference(set([column for column in valid_cols])))
    assert (col_not_valids == set()), 'Error: The column provided is not a column string: %s' % col_not_valids

    # User defined function to search cell value in list provide by user:
    if isinstance(replace_by, str) and look_up_key is not None:

        def check(cell):
            if cell is not None and (cell in look_up_key):
                return replace_by
            else:
                return cell

        func = udf(lambda cell: check(cell), StringType())
    else:
        def replace_from_dic(str_test):
            for key in replace_by.keys():
                if str_test in replace_by[key]:
                    str_test = key
            return str_test

        func = udf(lambda cell: replace_from_dic(cell) if cell is not None else cell, StringType())

    # Calling udf for each row of column provided by user. The rest of dataFrame is
    # maintained the same.
    exprs = [func(col(c)).alias(c) if c == columns[0] else c for c in self._df.columns]

    self._df = self._df.select(*exprs)

    self._add_transformation()  # checkpoint in case

    return self


@add_method(DataFrame)
def astype(self, cols_and_types):
    """
    Cast a column to a var type
    :param self:
    :param cols_and_types:
            List of tuples of column names and types to be casted. This variable should have the
            following structure:

            colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]

            The first parameter in each tuple is the column name, the second is the final datatype of column after
            the transformation is made.
    :return:
    """

    # Check if columnNames to be process are in dataframe
    column_names = self._parse_columns(cols_and_types, 0)

    # With need to handle the cols that we are not to cast
    new_col = ""

    columns = []
    for c in self.columns:
        for t in cols_and_types:
            # if the column is in cols_and_types
            if t[0] == c:
                new_col = col(c).cast(DICT_TYPES[TYPES[t[1]]]).alias(c)
            else:
                # Else, Add the column without modification
                new_col = col(c)
        columns.append(new_col)

    return self[columns]


@add_method(DataFrame)
def replace(self, search, change_to, columns):
    """

    :param self:
    :param search:
    :param change_to:
    :param columns:
    :return:
    """

    columns = self._parse_columns(columns)

    return self.replace(search, change_to, subset=columns)


@add_method(DataFrame)
def move_col(self, column, ref_col, position):
    """
    This function change a column position in dataFrame.
    :param self:
    :param column:
    :param ref_col:
    :param position: before and after the reference column
    :return:
    """

    # Check that column is a string or a list
    column = self._parse_columns(column)
    ref_col = self._parse_columns(ref_col)

    assert len(column) == 1, "Error: column must be a string or a list of one element"
    assert len(ref_col) == 1, "Error: ref_col must be a string or a list of one element"

    # Asserting if position is 'after' or 'before'
    assert (position == 'after') or (
            position == 'before'), "Error: Position parameter only can be 'after' or 'before' actually" % position

    # Get dataframe columns
    columns = self.columns

    # Get source and reference column index position
    new_index = columns.index(ref_col[0])
    old_index = columns.index(column[0])

    # if position is 'after':
    if position == 'after':
        # Check if the movement is from right to left:
        if new_index >= old_index:
            columns.insert(new_index, columns.pop(old_index))  # insert and delete a element
        else:  # the movement is form left to right:
            columns.insert(new_index + 1, columns.pop(old_index))
    else:  # If position if before:
        if new_index[0] >= old_index:  # Check if the movement if from right to left:
            columns.insert(new_index - 1, columns.pop(old_index))
        elif new_index[0] < old_index:  # Check if the movement if from left to right:
            columns.insert(new_index, columns.pop(old_index))

    return self[columns]


@add_method(DataFrame)
def _parse_columns(self, columns, index=None):
    """
    Check that a column list is a valis list of columns.
    :param columns:  Acepts * as param to return all the string columns in the dataframe
    :return: A list of columns string names
    """

    # Verify that columns are a string or list of string
    assert_type_str_or_list(columns)

    # if columns value is * get all dataframes columns
    if columns == "*":
        columns = list(map(lambda t: t[0], self.dtypes))

    # if string convert to list. Because we always return a list

    if isinstance(columns, str):
        columns = [columns]

    # Verify if we have a list
    elif isinstance(columns, list):
        # Verify that we have list inside the tuples
        if all(isinstance(x, tuple) for x in columns):
            # Extract a specific position in the tupple
            columns = [c[index] for c in columns]

    # Validate that all the columns exist
    validate_columns_names(self, columns)

    return columns


@add_method(DataFrame)
def lower(self, columns):
    """
    Lowercase all the string in a column
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, F.lower)


@add_method(DataFrame)
def upper(self, columns):
    """
    Uppercase all the strings column
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, F.upper)


@add_method(DataFrame)
def trim(self, columns):
    """
    Trim the string in a column
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, F.trim)


@add_method(DataFrame)
def reverse(self, columns):
    """
    Reverse the order of all the string in a column
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, F.reverse)


def _remove_accents(input_str):
    """
    Remove accents to a string
    :return:
    """
    # first, normalize strings:

    nfkd_str = unicodedata.normalize('NFKD', input_str)

    # Keep chars that has no other char combined (i.e. accents chars)
    with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])

    return with_out_accents


@add_method(DataFrame)
def remove_accents(self, columns):
    """
    Remove accents in specific columns
    :param columns:
    :return:
    """
    return self.apply_to_row(columns, _remove_accents)



@add_method(DataFrame)
def apply_to_row(self, columns, func):
    """
    Apply the func function to a serie of row in specific columns
    :param columns:
    :param func:
    :return:
    """

    columns = self._parse_columns(columns)

    for column in columns:
        self = self.withColumn(column, func(col(column)))
    return self


@add_method(DataFrame)
# FIX: We must find the better approach to fix the collition between the spark dataframe and optimus
def drop_column(self, columns):
    """

    :param columns: *, string or string or columns list to be dropped
    :return: Dataframe
    """
    columns = self._parse_columns(columns)

    for column in columns:
        self = self.drop(column)
    return self


@add_method(DataFrame)
def drop(self, func):
    """This function is an alias of filter and where spark functions.
           :param func     func must be an expression with the following form:

                   func = col('col_name') > value.

                   func is an expression where col is a pyspark.sql.function.
           """
    self = self.filter(func)

    # Returning the transformer object for able chaining operations
    return self


@add_method(DataFrame)
def drop_duplicates(self, columns=None):
    """

    :param cols: List of columns to make the comparison, this only  will consider this subset of columns,
    for dropping duplicates. The default behavior will only drop the identical rows.
    :return: Return a new DataFrame with duplicate rows removed
    """

    columns = self._parse_columns(columns)

    return self.drop_duplicates(columns)


@add_method(DataFrame)
def drop_empty_rows(self, columns, how="all"):
    """
    Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
    'any' of the values is null.

    :param how: ‘any’ or ‘all’. If ‘any’, drop a row if it contains any nulls. If ‘all’, drop a row only if all its
    values are null. The default is 'all'.
    :return: Returns a new DataFrame omitting rows with null values.
    """

    assert isinstance(how, str), "Error, how argument provided must be a string."

    assert how == 'all' or (
            how == 'any'), "Error, how only can be 'all' or 'any'."

    columns = self._parse_columns(columns)

    return self._df.dropna(how, columns)




@add_method(DataFrame)
def keep(self, columns):
    """
    Just Keep the columns and drop.
    :param columns:
    :return:
    """

    columns = self._parse_columns(columns)
    return self.select(*columns)


@add_method(DataFrame)
def rename(self, columns_pair):
    """"
    This functions change the name of a column(s) datraFrame.
    :param columns_pair: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
    """
    # Check that the 1st element in the tuple is a valis set of columns
    columns = self._parse_columns(columns_pair, 0)

    # Rename cols
    columns = [col(column[0]).alias(column[1]) for column in columns_pair]

    return self.select(columns)


# Quantile statistics
@add_method(DataFrame)
def _agg(self, agg, columns):
    """
    Helper function to manage aggregation functions
    :param agg: Aggregation function from spark
    :param columns: list of columns names or a string (a column name).
    :return:
    """
    columns = self._parse_columns(columns)

    # Return the min value
    r = list(map(lambda c: self.agg({c: agg}).collect()[0][0], columns))

    # if the list has one elment return just a single element
    return one_list_to_val(r)


@add_method(DataFrame)
def min(self, columns):
    """
    Return the min value from a column dataframe
    :param columns: '*', list of columns names or a string (a column name).
    :return:
    """
    return self._agg("min", columns)


@add_method(DataFrame)
def max(self, columns):
    """
    Return the max value from a column dataframe
    :param columns: '*', list of columns names or a string (a column name).
    :return:
    """
    return self._agg("max", columns)


@add_method(DataFrame)
def range(self, columns):
    """
    Return the range form the min to the max value
    :param columns:
    :return:
    """

    columns = self._parse_columns(columns)

    # if max_val, min_val has 1 element convert to a list
    max_val = val_to_list(self.max(columns))
    min_val = val_to_list(self.min(columns))

    # Substract max and min lists to get the range
    return one_list_to_val([x - y for x, y in zip(max_val, min_val)])


@add_method(DataFrame)
def median(self, columns):
    """
    Return the median of a column dataframe
    :param columns:
    :return:
    """

    return self.approxQuantile(columns, [0.5], 0)[0]


# Descriptive Analytics
@add_method(DataFrame)
def stddev(self, columns):
    """
    Return the standard deviation of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("stddev", columns)


@add_method(DataFrame)
def kurt(self, columns):
    """
    Return the kurtosis of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("kurtosis", columns)


@add_method(DataFrame)
def mean(self, columns):
    """
    Return the mean of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("mean", columns)


@add_method(DataFrame)
def skewness(self, columns):
    """
    Return the skewness of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("skewness", columns)


@add_method(DataFrame)
def sum(self, columns):
    """
    Return the sum of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("skewness", columns)


@add_method(DataFrame)
def variance(self, columns):
    """
    Return the variance of a column dataframe
    :param columns:
    :return:
    """
    return self._agg("variance", columns)


@add_method(DataFrame)
def select_idx(self, indexes):
    """
    Select specified columns by index.
    :param indexes: Indices to select from DF.
    :return: Dataframe with selected columns.
    """

    assert isinstance(indexes, list), "Error: indices must a list"

    if isinstance(indexes, int):
        indexes = [indexes]

    self._df = self._df.select(*(self._df.columns[i] for i in indexes))

    return self


# Alias for select_idx
iloc = select_idx


def operation_in_type(self, parameters):
    """ This function makes operations in a columnType of dataframe. It is well know that DataFrames are consistent,
    but it in this context, operation are based in types recognized by the dataframe analyzer, types are identified
    according if the value is parsable to int or float, etc.

    This functions makes the operation in column elements that are recognized as the same type that the data_type
    argument provided in the input function.

    Columns provided in list of tuples cannot be repeated
    :param parameters   List of columns in the following form: [(columnName, data_type, func),
                                                                (columnName1, dataType1, func1)]
    :return None
    """

    def check_data_type(value):

        try:  # Try to parse (to int) register value
            int(value)
            # Add 1 if suceed:
            return 'integer'
        except ValueError:
            try:
                # Try to parse (to float) register value
                float(value)
                # Add 1 if suceed:
                return 'float'
            except ValueError:
                # Then, it is a string
                return 'string'
        except TypeError:
            return 'null'

    types = {type('str'): 'string', type(1): 'int', type(1.0): 'float'}

    exprs = []
    for column, data_type, func in parameters:
        # Cheking if column name is string datatype:
        self._assert_type_str(column, "columnName")
        # Checking if column exists in dataframe:
        assert column in self._df.columns, \
            "Error: Column %s specified as columnName argument does not exist in dataframe" % column
        # Checking if column has a valid datatype:
        assert (data_type in ['integer', 'float', 'string',
                              'null']), \
            "Error: data_type only can be one of the followings options: integer, float, string, null."
        # Checking if func parameters is func data_type or None
        assert isinstance(func, type(None)) or isinstance(func, type(lambda x: x)), \
            "func argument must be a function or NoneType"

        if 'function' in str(type(func)):
            func_udf = udf(lambda x: func(x) if check_data_type(x) == data_type else x)

        if isinstance(func, str) or isinstance(func, int) or isinstance(func, float):
            assert [x[1] in types[type(func)] for x in filter(lambda x: x[0] == columnName, self._df.dtypes)][
                0], \
                "Error: Column of operation and func argument must be the same global type. " \
                "Check column type by df.printSchema()"
            func_udf = udf(lambda x: func if check_data_type(x) == data_type else x)

        if func is None:
            func_udf = udf(lambda x: None if check_data_type(x) == data_type else x)

        exprs.append(func_udf(col(column)).alias(column))

    col_not_provided = [x for x in self._df.columns if x not in [column[0] for column in parameters]]

    self._df = self._df.select(col_not_provided + [*exprs])
    self._add_transformation()  # checkpoint in case

    return self

def row_filter_by_type(self, column_name, type_to_delete):
    """This function has built in order to deleted some type of dataframe """
    # Check if column_name argument a string datatype:
    self._assert_type_str(column_name, "column_name")
    # Asserting if column_name exits in dataframe:
    assert column_name in self._df.columns, \
        "Error: Column specified as column_name argument does not exist in dataframe"
    # Check if type_to_delete argument a string datatype:
    self._assert_type_str(type_to_delete, "type_to_delete")
    # Asserting if dataType argument has a valid type:
    assert (type_to_delete in ['integer', 'float', 'string',
                               'null']), \
        "Error: dataType only can be one of the followings options: integer, float, string, null."

    # Function for determine if register value is float or int or string:
    def data_type(value):

        try:  # Try to parse (to int) register value
            int(value)
            # Add 1 if suceed:
            return 'integer'
        except ValueError:
            try:
                # Try to parse (to float) register value
                float(value)
                # Add 1 if suceed:
                return 'float'
            except ValueError:
                # Then, it is a string
                return 'string'
        except TypeError:
            return 'null'

    func = udf(data_type, StringType())
    self._df = self._df.withColumn('types', func(col(column_name))).where((col('types') != type_to_delete)).drop(
        'types')
    self._add_transformation()  # checkpoint in case

    return self

def split_str_col(self, column, feature_names, mark):
    """This functions split a column into different ones. In the case of this method, the column provided should
    be a string of the following form 'word,foo'.

    :param column       Name of the target column, this column is going to be replaced.
    :param feature_names     List of strings of the new column names after splitting the strings.
    :param mark         String that specifies the splitting mark of the string, this frequently is ',' or ';'.
    """

    # Check if column argument is a string datatype:
    self._assert_type_str(column, "column")

    # Check if mark argument is a string datatype:
    self._assert_type_str(mark, "mark")

    assert (column in self._df.columns), "Error: column specified does not exist in dataFrame."

    assert (isinstance(feature_names, list)), "Error: feature_names must be a list of strings."

    # Setting a udf that split the string into a list of strings.
    # This is "word, foo" ----> ["word", "foo"]
    func = udf(lambda x: x.split(mark), ArrayType(StringType()))

    self._df = self._df.withColumn(column, func(col(column)))
    self.undo_vec_assembler(column=column, feature_names=feature_names)
    self._add_transformation()  # checkpoint in case

    return self

def count_items(self, col_id, col_search, new_col_feature, search_string):
    """
    This function can be used to create Spark DataFrames with frequencies for picked values of
    selected columns.

    :param col_id    column name of the columnId of dataFrame
    :param col_search     column name of the column to be split.
    :param new_col_feature        Name of the new column.
    :param search_string         string of value to be counted.

    :returns Spark Dataframe.

    Please, see documentation for more explanations about this method.

    """
    # Asserting if position is string or list:

    assert isinstance(search_string, str), "Error: search_string argument must be a string"

    # Asserting parameters are not empty strings:
    assert (
            (col_id != '') and (col_search != '') and (new_col_feature != '')), \
        "Error: Input parameters can't be empty strings"

    # Check if col_search argument is string datatype:
    self._assert_type_str(col_search, "col_search")

    # Check if new_col_feature argument is a string datatype:
    self._assert_type_str(new_col_feature, "new_col_feature")

    # Check if col_id argument is a string datatype:
    self._assert_type_str(col_id, "col_id")

    # Check if col_id to be process are in dataframe
    self._assert_cols_in_df(columns_provided=[col_id], columns_df=self._df.columns)

    # Check if col_search to be process are in dataframe
    self._assert_cols_in_df(columns_provided=[col_search], columns_df=self._df.columns)

    # subset, only PAQ and Tipo_Unidad:
    subdf = self._df.select(col_id, col_search)

    # subset de
    new_column = subdf.where(subdf[col_search] == search_string).groupBy(col_id).count()

    # Left join:
    new_column = new_column.withColumnRenamed(col_id, col_id + '_other')

    exprs = (subdf[col_id] == new_column[col_id + '_other']) & (subdf[col_search] == search_string)

    df_mod = subdf.join(new_column, exprs, 'left_outer')

    # Cleaning dataframe:
    df_mod = df_mod.drop(col_id + '_other').drop(col_search).withColumnRenamed('count', new_col_feature) \
        .dropna("any")

    print("Counting existing " + search_string + " in " + col_search)
    return df_mod.sort(col_id).drop_duplicates([col_id])