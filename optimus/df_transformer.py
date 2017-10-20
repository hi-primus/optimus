# Importing sql types
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType
# Importing sql functions
from pyspark.sql.functions import col, udf, trim, lit, format_number, months_between, date_format, unix_timestamp, \
    current_date, abs as mag
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
import re
import string
import unicodedata
import pyspark.sql.dataframe
from pyspark.ml.feature import Imputer


class DataFrameTransformer:
    """DataFrameTransformer is a class to make transformations in dataFrames"""

    def __init__(self, df):
        """Class constructor.
        :param  df      DataFrame to be transformed.
        """
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), \
            "Error, df argument must be a pyspark.sql.dataframe.DataFrame instance"

        # Dataframe
        self._df = df
        # SparkContext:
        self._sql_context = self._df.sql_ctx
        self._number_of_transformations = 0

    @classmethod
    def _assert_type_str_or_list(cls, variable, name_arg):
        """This function asserts if variable is a string or a list dataType."""
        assert isinstance(variable, (str, list)), \
            "Error: %s argument must be a string or a list." % name_arg

    @classmethod
    def _assert_type_int_or_float(cls, variable, name_arg):
        """This function asserts if variable is a string or a list dataType."""
        assert isinstance(variable, (int, float)), \
            "Error: %s argument must be a int or a float." % name_arg

    @classmethod
    def _assert_type_str(cls, variable, name_arg):
        """This function asserts if variable is a string or a list dataType."""
        assert isinstance(variable, str), \
            "Error: %s argument must be a string." % name_arg

    @classmethod
    def _assert_cols_in_df(cls, columns_provided, columns_df):
        """This function asserts if columns_provided exists in dataFrame.
        Inputs:
        columns_provided: the list of columns to be process.
        columns_df: list of columns's dataFrames
        """
        col_not_valids = (
            set([column for column in columns_provided]).difference(set([column for column in columns_df])))
        assert (col_not_valids == set()), 'Error: The following columns do not exits in dataFrame: %s' % col_not_valids

    def _add_transformation(self):
        self._number_of_transformations += 1

        if self._number_of_transformations > 50:
            self.check_point()
            self._number_of_transformations = 0

    def set_data_frame(self, df):
        """This function set a dataframe into the class for subsequent actions.
        """
        assert isinstance(df, pyspark.sql.dataframe.DataFrame), "Error: df argument must a sql.dataframe type"
        self._df = df

    @property
    def get_data_frame(self):
        """This function return the dataframe of the class
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        return self._df

    def show(self, n=10, truncate=True):
        """This function shows the dataframe of the class
        :param n: number or rows to show
        :param truncate: If set to True, truncate strings longer than 20 chars by default.
        :rtype: pyspark.sql.dataframe.DataFrame.show()
        """
        return self._df.show(n, truncate)

    def lower_case(self, columns):
        """This function set all strings in columns of dataframe specified to lowercase.
        Columns argument must be a string or a list of string. In order to apply this function to all
        dataframe, columns must be equal to '*'"""

        func = lambda cell: cell.lower() if cell is not None else cell
        self.set_col(columns, func, 'string')
        return self

    def upper_case(self, columns):
        """This function set all strings in columns of dataframe specified to uppercase.
        Columns argument must be a string or a list of string. In order to apply this function to all
        dataframe, columns must be equal to '*'"""
        func = lambda cell: cell.upper() if cell is not None else cell
        self.set_col(columns, func, 'string')
        return self

    def impute_missing(self, columns, out_cols, strategy):
        """
        Imputes missing data from specified columns using the mean or median.
        :param columns: List of columns to be analyze.
        :param out_cols: List of output columns with missing values imputed.
        :param strategy: String that specifies the way of computing missing data. Can be "mean" or "median"
        :return: Transformer object (DF with columns that has the imputed values).
        """

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        assert isinstance(columns, list), "Error: columns argument must be a list"

        assert isinstance(out_cols, list), "Error: out_cols argument must be a list"

        # Check if columns argument a string datatype:
        self._assert_type_str(strategy, "strategy")

        assert (strategy == "mean" or strategy == "median"), "Error: strategy has to be 'mean' or 'median'."

        def impute(cols):
            imputer = Imputer(inputCols=cols, outputCols=out_cols)
            model = imputer.setStrategy(strategy).fit(self._df)
            self._df = model.transform(self._df)

        impute(columns)

        return self

    def replace_na(self, value, columns=None):
        """
        Replace nulls with specified value.
        :param columns: optional list of column names to consider. Columns specified in subset that do not have
        matching data type are ignored. For example, if value is a string, and subset contains a non-string column,
         then the non-string column is simply ignored.
        :param value: Value to replace null values with. If the value is a dict, then subset is ignored and value
         must be a mapping from column name (string) to replacement value. The replacement
         value must be an int, long, float, or string.
        :return: Transformer object (DF with columns with replaced null values).
        """

        if columns == "*":
            columns = None

        # Columns to list
        if isinstance(columns, str):
            columns = [columns]

        if columns is not None:
            assert isinstance(columns, list), "Error: columns argument must be a list"

        assert isinstance(value, (int, float, str, dict)), "Error: value argument must be an " \
                                                           "int, long, float, string, or dict"

        def replace_it(val, col):
            self._df = self._df.fillna(val, subset=col)

        replace_it(val=value, col=columns)

        self._add_transformation()

        return self

    def check_point(self):
        """This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
        evaluation approach in processing data: transformation functions are not computed into an action is called.
        Sometimes when transformations are numerous, the computations are very extensive because the high number of
        operations that spark needs to run in order to get the results.

        Other important thing is that apache spark usually save task but not result of dataFrame, so tasks are
        accumulated and the same situation happens.

        The problem can be deal it with the checkPoint method. This method save the resulting dataFrame in disk, so
         the lineage is cut.
        """

        # Checkpointing of dataFrame. One question can be thought. Why not use cache() or persist() instead of
        # checkpoint. This is because cache() and persis() apparently do not break the lineage of operations,
        print("Saving changes at disk by checkpoint...")
        self._df.checkpoint()
        self._df.count()
        self._df = self._sql_context.createDataFrame(self._df, self._df.schema)
        print("Done.")

    execute = check_point

    def trim_col(self, columns):
        """This methods cut left and right extra spaces in column strings provided by user.
        :param columns   list of column names of dataFrame.
                If a string "*" is provided, the method will do the trimming operation in whole dataFrame.

        :return transformer object
        """

        # Function to trim spaces in columns with strings datatype
        def col_trim(columns):
            exprs = [trim(col(c)).alias(c)
                     if (c in columns) and (c in valid_cols)
                     else c
                     for (c, t) in self._df.dtypes]
            self._df = self._df.select(*exprs)

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols

        # Columns
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        # Trimming spaces in columns:
        col_trim(columns)

        self._add_transformation()

        # Returning the transformer object for able chaining operations
        return self

    def drop_col(self, columns):
        """This method eliminate the list of columns provided by user.
        :param columns      list of columns names or a string (a column name).

        :return transformer object
        """

        def col_drop(columns):
            exprs = filter(lambda c: c not in columns, self._df.columns)
            self._df = self._df.select(*exprs)

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Columns
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        # Calling colDrop function
        col_drop(columns)

        self._add_transformation()

        # Returning the transformer object for able chaining operations
        return self

    def replace_col(self, search, change_to, columns):
        """This method search the 'search' value in DataFrame columns specified in 'columns' in order to replace it
        for 'change_to' value.


        :param search       value to search in dataFrame.
        :param change_to     value used to replace the old one in dataFrame.
        :param columns      list of string column names or a string (column name). If columns = '*' is provided,
                            searching and replacing action is made in all columns of DataFrame that have same
                            dataType of search and change_to.

        search and change_to arguments are expected to be numbers and same dataType ('integer', 'string', etc) each other.
        olumns argument is expected to be a string or list of string column names.

        :return transformer object
        """

        def col_replace(columns):
            self._df = self._df.replace(search, change_to, subset=columns)

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Asserting search parameter is a string or a number
        assert isinstance(search, str) or isinstance(search, float) or isinstance(search,
                                                                                  int), \
            "Error: Search parameter must be a number or string"

        # Asserting change_to parameter is a string or a number
        assert isinstance(change_to, str) or isinstance(change_to, float) or isinstance(change_to,
                                                                                        int), \
            "Error: change_to parameter must be a number or string"

        # Asserting search and change_to have same type
        assert isinstance(search, type(change_to)), \
            'Error: Search and ChangeTo must have same datatype: Integer, String, Float'

        # Change
        types = {type(''): 'string', type(int(1)): 'int', type(float(1.2)): 'float', type(1.2): 'double'}

        valid_cols = [c for (c, t) in filter(lambda t: t[1] == types[type(search)], self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
            col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % \
                                      col_not_valids

        col_replace(columns)

        self._add_transformation()

        # Returning the transformer object for able chaining operations
        return self

    def delete_row(self, func):
        """This function is an alias of filter and where spark functions.
        :param func     func must be an expression with the following form:

                func = col('col_name') > value.

                func is an expression where col is a pyspark.sql.function.
        """
        self._df = self._df.filter(func)

        self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def set_col(self, columns, func, data_type):
        """This method can be used to make math operations or string manipulations in row of dataFrame columns.

        :param columns      list of columns (or a single column) of dataFrame.
        :param func         function or string type which describe the data_type that func function should return.
        :param data_type     string indicating one of the following options: 'integer', 'string', 'double','float'.

        'columns' argument is expected to be a string or a list of columns names.
        It is a requirement for this method that the data_type provided must be the same to data_type of columns.
        On the other hand, if user writes columns == '*' the method makes operations in func if only if columns
        have same data_type that data_type argument.

        :return transformer object
        """
        dict_types = {'string': StringType(), 'str': StringType(), 'integer': IntegerType(),
                      'int': IntegerType(), 'float': FloatType(), 'double': DoubleType(), 'Double': DoubleType()}

        types = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int',
                 'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}

        try:
            function = udf(func, dict_types[data_type])
        except KeyError:
            assert False, "Error, data_type not recognized"

        def col_set(columns, function):
            exprs = [function(col(c)).alias(c) if c in columns else c for (c, t) in self._df.dtypes]
            try:
                self._df = self._df.select(*exprs)
            except Exception as e:
                print(e)
                assert False, "Error: Make sure if operation is compatible with row datatype."

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == types[data_type], self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
            col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
                                      % col_not_valids

        col_set(columns, function)

        self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    # Drop
    def keep_col(self, columns):
        """This method keep only columns specified by user with columns argument in DataFrame.
        :param columns list of columns or a string (column name).

        :return transformer object
        """

        def col_keep(columns):
            exprs = filter(lambda c: c in columns, self._df.columns)
            self._df = self._df.select(*exprs)

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Check is column if a string.
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        # Calling colDrop function
        col_keep(columns)

        self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def clear_accents(self, columns):
        """This function deletes accents in strings column dataFrames, it does not eliminate main characters,
        but only deletes special tildes.

        :param columns  String or a list of column names.

        """

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
            col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
                                      % col_not_valids

        # Receives  a string as an argument
        def remove_accents(input_str):
            # first, normalize strings:
            nfkd_str = unicodedata.normalize('NFKD', input_str)
            # Keep chars that has no other char combined (i.e. accents chars)
            with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
            return with_out_accents

        function = udf(lambda x: remove_accents(x) if x is not None else x, StringType())
        exprs = [function(col(c)).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]
        self._df = self._df.select(*exprs)

        self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def remove_special_chars(self, columns):
        """This function remove special chars in string columns, such as: .!"#$%&/()
        :param columns      list of names columns to be processed.

        columns argument can be a string or a list of strings."""

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str):
            columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
            col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
                                      % col_not_valids

        def rm_spec_chars(input_str):
            # Remove all punctuation and control characters
            for punct in (set(input_str) & set(string.punctuation)):
                input_str = input_str.replace(punct, "")
            return input_str

        # User define function that does operation in cells
        function = udf(lambda cell: rm_spec_chars(cell) if cell is not None else cell, StringType())

        exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols)  else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def remove_special_chars_regex(self, columns, regex):
        """This function remove special chars in string columns using a regex, such as: .!"#$%&/()
        :param columns      list of names columns to be processed.
        :param regex        string that contains the regular expression

        columns argument can be a string or a list of strings."""

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str):
            columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
            col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
                                      % col_not_valids

        def rm_spec_chars_regex(input_str, regex):
            for _ in set(input_str):
                input_str = re.sub(regex, '', input_str)
            return input_str

        # User define function that does operation in cells
        function = udf(lambda cell: rm_spec_chars_regex(cell, regex) if cell is not None else cell, StringType())

        exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols)  else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def rename_col(self, columns):
        """"This functions change the name of columns datraFrame.
        :param columns      List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).

        """
        # Asserting columns is string or list:
        assert isinstance(columns, list) and isinstance(columns[0], tuple), \
            "Error: Column argument must be a list of tuples"

        col_not_valids = (
            set([column[0] for column in columns]).difference(set([column for column in self._df.columns])))

        assert (col_not_valids == set()), 'Error: The following columns do not exits in dataFrame: %s' % col_not_valids

        old_names = [column[0] for column in columns]

        not_in_type = filter(lambda c: c not in old_names, self._df.columns)

        exprs = [col(column[0]).alias(column[1]) for column in columns] + [col(column) for column in not_in_type]

        self._add_transformation()  # checkpoint in case

        self._df = self._df.select(*exprs)
        return self

    def lookup(self, column, str_to_replace, list_str=None):
        """This method search a list of strings specified in `list_str` argument among rows
        in column dataFrame and replace them for `str_to_replace`.

        :param  column      Column name, this variable must be string dataType.
        :param  str_to_replace    string that going to replace all others present in list_str argument
        :param  list_str     List of strings to be replaced

        `lookup` can only be runned in StringType columns.


        """
        # Check if columns argument a string datatype:
        self._assert_type_str(column, "column")

        # Asserting columns is string or list:
        assert isinstance(str_to_replace, (str, dict)), "Error: str_to_replace argument must be a string or a dict"

        if isinstance(str_to_replace, dict):
            assert (str_to_replace != {}), "Error, str_to_replace must be a string or a non empty python dictionary"
            assert (
                list_str is None), "Error, If a python dictionary if specified, list_str argument must be None: list_str=None"

        # Asserting columns is string or list:
        assert isinstance(list_str, list) and list_str != [] or (
            list_str is None), "Error: Column argument must be a non empty list"

        if isinstance(str_to_replace, str):
            assert list_str is not None, "Error: list_str cannot be None if str_to_replace is a String, please you need to specify \
             the list_str string"

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        if isinstance(column, str):
            column = [column]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=column, columns_df=self._df.columns)

        # Asserting if selected column datatype and search and changeTo parameters are the same:
        col_not_valids = (set(column).difference(set([column for column in valid_cols])))
        assert (col_not_valids == set()), 'Error: The column provided is not a column string: %s' % col_not_valids

        # User defined function to search cell value in list provide by user:
        if isinstance(str_to_replace, str) and list_str is not None:

            def check(cell):
                if cell is not None and (cell in list_str):
                    return str_to_replace
                else:
                    return cell

            func = udf(lambda cell: check(cell), StringType())
        else:
            def replace_from_dic(str_test):
                for key in str_to_replace.keys():
                    if str_test in str_to_replace[key]:
                        str_test = key
                return str_test

            func = udf(lambda cell: replace_from_dic(cell) if cell is not None else cell, StringType())

        # Calling udf for each row of column provided by user. The rest of dataFrame is
        # maintained the same.
        exprs = [func(col(c)).alias(c) if c == column[0] else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        self._add_transformation()  # checkpoint in case

        return self

    def move_col(self, column, ref_col, position):
        """This funcion change column position in dataFrame.
        :param column:   Name of the column to be moved in dataFrame. column argument must be a string.
        :param ref_col:   Name of reference column in dataFrame. This column will be a reference to place the
                        column to be moved.
        :param position: Can be one of the following options: 'after' or 'before'. If 'after' is provided, column
                        provided will be placed just after the ref_col selected."""
        # Columns of dataFrame
        columns = self._df.columns

        # Check if columns argument a string datatype:
        self._assert_type_str(column, "column")

        # Check if column to be process are in dataframe
        self._assert_cols_in_df(columns_provided=[column], columns_df=self._df.columns)

        # Check if columns argument a string datatype:
        self._assert_type_str(ref_col, "ref_col")

        # Asserting parameters are not empty strings:
        assert (
            (column != '') and (ref_col != '') and (position != '')), "Error: Input parameters can't be empty strings"

        # Check if ref_col is in dataframe
        self._assert_cols_in_df(columns_provided=[ref_col], columns_df=self._df.columns)

        # Check if columns argument a position string datatype:
        self._assert_type_str(position, "position")

        # Asserting if position is 'after' or 'before'
        assert (position == 'after') or (
            position == 'before'), "Error: Position parameter only can be 'after' or 'before'"

        # Finding position of column to move:
        find_col = lambda columns, column: [index for index, c in enumerate(columns) if c == column]
        new_index = find_col(columns, ref_col)
        old_index = find_col(columns, column)

        # if position is 'after':
        if position == 'after':
            # Check if the movement is from right to left:
            if new_index[0] >= old_index[0]:
                columns.insert(new_index[0], columns.pop(old_index[0]))  # insert and delete a element
            else:  # the movement is form left to right:
                columns.insert(new_index[0] + 1, columns.pop(old_index[0]))
        else:  # If position if before:
            if new_index[0] >= old_index[0]:  # Check if the movement if from right to left:
                columns.insert(new_index[0] - 1, columns.pop(old_index[0]))
            elif new_index[0] < old_index[0]:  # Check if the movement if from left to right:
                columns.insert(new_index[0], columns.pop(old_index[0]))

        self._df = self._df[columns]

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

    def date_transform(self, columns, current_format, output_format):
        """
        :param  columns     Name date columns to be transformed. Columns ha
        :param  current_format   current_format is the current string dat format of columns specified. Of course,
                                all columns specified must have the same format. Otherwise the function is going
                                to return tons of null values because the transformations in the columns with
                                different formats will fail.
        :param  output_format    output date string format to be expected.
        """
        # Check if current_format argument a string datatype:
        self._assert_type_str(current_format, "current_format")
        # Check if output_format argument a string datatype:
        self._assert_type_str(output_format, "output_format")
        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        if isinstance(columns, str):
            columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        exprs = [date_format(unix_timestamp(c, current_format).cast("timestamp"), output_format).alias(
            c) if c in columns else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        self._add_transformation()  # checkpoint in case

        return self

    def age_calculate(self, column, dates_format, name_col_age):
        """
        This method compute the age of clients based on their born dates.
        :param  column      Name of the column born dates column.
        :param  dates_format  String format date of the column provided.
        :param  name_col_age  Name of the new column, the new columns is the resulting column of ages.

        """
        # Check if column argument a string datatype:
        self._assert_type_str(column, "column")

        # Check if dates_format argument a string datatype:
        self._assert_type_str(dates_format, "dates_format")

        # Asserting if column if in dataFrame:
        assert column in self._df.columns, "Error: Column assigned in column argument does not exist in dataFrame"

        # Output format date
        format_dt = "yyyy-MM-dd"  # Some SimpleDateFormat string

        exprs = format_number(
            mag(
                months_between(date_format(
                    unix_timestamp(column, dates_format).cast("timestamp"), format_dt), current_date()) / 12), 4).alias(
            name_col_age)

        self._df = self._df.withColumn(name_col_age, exprs)

        self._add_transformation()  # checkpoint in case

        return self

    def cast_func(self, cols_and_types):
        """

        :param cols_and_types     List of tuples of column names and types to be casted. This variable should have the
                                following structure:

                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]

                The first parameter in each tuple is the column name, the second is the finale datatype of column after
                the transformation is made.
        :return:
        """

        dict_types = {'string': StringType(), 'str': StringType(), 'integer': IntegerType(),
                      'int': IntegerType(), 'float': FloatType(), 'double': DoubleType(), 'Double': DoubleType()}

        types = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int',
                 'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}

        # Asserting cols_and_types is string or list:
        assert isinstance(cols_and_types, (str, list)), "Error: Column argument must be a string or a list."

        if isinstance(cols_and_types, str):
            cols_and_types = [cols_and_types]

        column_names = [column[0] for column in cols_and_types]

        # Check if columnNames to be process are in dataframe
        self._assert_cols_in_df(columns_provided=column_names, columns_df=self._df.columns)

        not_specified_columns = filter(lambda c: c not in column_names, self._df.columns)

        exprs = [col(column[0]).cast(dict_types[types[column[1]]]).alias(column[0]) for column in cols_and_types] + [
            col(column) for column in not_specified_columns]

        self._df = self._df.select(*exprs)
        self._add_transformation()  # checkpoint in case

        return self

    # This function replace a string specified
    def empty_str_to_str(self, columns, custom_str):

        # Check if custom_str argument a string datatype:
        self._assert_type_str(custom_str, "custom_str")

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = valid_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str):
            columns = [columns]

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        def blank_as_null(x):
            return when(col(x) != "", col(x)).otherwise(custom_str)

        exprs = [blank_as_null(c).alias(c) if (c in columns) and (c in valid_cols)  else c for c in self._df.columns]

        self._df = self._df.select(*exprs)
        self._add_transformation()  # checkpoint in case

        return self

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

    def undo_vec_assembler(self, column, feature_names):
        """This function unpack a column of list arrays into different columns.
        +-------------------+-------+
        |           features|columna|
        +-------------------+-------+
        |[11, 2, 1, 1, 1, 1]|   hola|
        | [0, 1, 1, 1, 1, 1]|  salut|
        |[31, 1, 1, 1, 1, 1]|  hello|
        +-------------------+-------+
                      |
                      |
                      V
        +-------+---+---+-----+----+----+---+
        |columna|one|two|three|four|five|six|
        +-------+---+---+-----+----+----+---+
        |   hola| 11|  2|    1|   1|   1|  1|
        |  salut|  0|  1|    1|   1|   1|  1|
        |  hello| 31|  1|    1|   1|   1|  1|
        +-------+---+---+-----+----+----+---+
        """
        # Check if column argument a string datatype:
        self._assert_type_str(column, "column")

        assert (column in self._df.columns), "Error: column specified does not exist in dataFrame."

        assert (isinstance(feature_names, list)), "Error: feature_names must be a list of strings."
        # Function to extract value from list column:
        func = udf(lambda x, index: x[index])

        exprs = []

        # Recursive function:
        def exprs_func(column, exprs, feature_names, index):
            if index == 0:
                return [func(col(column), lit(index)).alias(feature_names[index])]
            else:
                return exprs_func(column, exprs, feature_names, index - 1) + [
                    func(col(column), lit(index)).alias(feature_names[index])]

        self._df = self._df.select(
            [x for x in self._df.columns] + [*exprs_func(column, exprs, feature_names, len(feature_names) - 1)]).drop(
            column)
        self._add_transformation()  # checkpoint in case

        return self

    def scale_vec_col(self, columns, name_output_col):
        """
        This function groups the columns specified and put them in a list array in one column, then a scale
        process is made. The scaling proccedure is spark scaling default (see the example
        bellow).

        +---------+----------+
        |Price    |AreaLiving|
        +---------+----------+
        |1261706.9|16        |
        |1263607.9|16        |
        |1109960.0|19        |
        |978277.0 |19        |
        |885000.0 |19        |
        +---------+----------+

                    |
                    |
                    |
                    V
        +----------------------------------------+
        |['Price', 'AreaLiving']                 |
        +----------------------------------------+
        |[0.1673858972637624,0.5]                |
        |[0.08966137157852398,0.3611111111111111]|
        |[0.11587093205757598,0.3888888888888889]|
        |[0.1139820728616421,0.3888888888888889] |
        |[0.12260126542983639,0.4722222222222222]|
        +----------------------------------------+
        only showing top 5 rows

        """

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(columns, "columns")

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        # Check if name_output_col argument a string datatype:
        self._assert_type_str(name_output_col, "nameOutpuCol")

        # Model to use vectorAssember:
        vec_assembler = VectorAssembler(inputCols=columns, outputCol="features_assembler")
        # Model for scaling feature column:
        mm_scaler = MinMaxScaler(inputCol="features_assembler", outputCol=name_output_col)
        # Dataframe with feature_assembler column
        temp_df = vec_assembler.transform(self._df)
        # Fitting scaler model with transformed dataframe
        model = mm_scaler.fit(temp_df)

        exprs = list(filter(lambda x: x not in columns, self._df.columns))

        exprs.extend([name_output_col])

        self._df = model.transform(temp_df).select(*exprs)
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

    def remove_empty_rows(self, how="all"):
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

        self._df = self._df.dropna(how)

        return self

    def remove_duplicates(self, cols=None):
        """

        :param cols: List of columns to make the comparison, this only  will consider this subset of columns,
        for dropping duplicates. The default behavior will only drop the identical rows.
        :return: Return a new DataFrame with duplicate rows removed
        """

        assert isinstance(cols, list), "Error, cols argument provided must be a list."

        self._df = self._df.drop_duplicates(cols)

        return self

    def write_df_as_json(self, path):
        p = re.sub("}\'", "}", re.sub("\'{", "{", str(self._df.toJSON().collect())))

        with open(path, 'w') as outfile:
            # outfile.write(str(json_cols).replace("'", "\""))
            outfile.write(p)

    def to_csv(self, path_name, header="true", mode="overwrite", sep=",", *args, **kargs):
        """
        Write dataframe as CSV.
        :param path_name: Path to write the DF and the name of the output CSV file.
        :param header: True or False to include header
        :param mode: Specifies the behavior of the save operation when data already exists.
                    "append": Append contents of this DataFrame to existing data.
                    "overwrite" (default case): Overwrite existing data.
                    "ignore": Silently ignore this operation if data already exists.
                    "error": Throw an exception if data already exists.
        :param sep: sets the single character as a separator for each field and value. If None is set,
        it uses the default value.
        :return: Dataframe in a CSV format in the specified path.
        """

        self._assert_type_str(path_name, "path_name")

        assert header == "true" or header == "false", "Error header must be 'true' or 'false'"

        if header == 'true':
            header = True
        else:
            header = False

        return self._df.write.options(header=header).mode(mode).csv(path_name, sep=sep, *args, **kargs)

    def string_to_index(self, input_cols):
        """
        Maps a string column of labels to an ML column of label indices. If the input column is
        numeric, we cast it to string and index the string values.
        :param input_cols: Columns to be indexed.
        :return: Dataframe with indexed columns.
        """

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(input_cols, "input_cols")

        if isinstance(input_cols, str):
            input_cols = [input_cols]

        from pyspark.ml import Pipeline
        from pyspark.ml.feature import StringIndexer

        indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(self._df) for column in
                    list(set(input_cols))]

        pipeline = Pipeline(stages=indexers)
        self._df = pipeline.fit(self._df).transform(self._df)

        return self

    def index_to_string(self, input_cols):
        """
        Maps a column of indices back to a new column of corresponding string values. The index-string mapping is
        either from the ML attributes of the input column, or from user-supplied labels (which take precedence over
        ML attributes).
        :param input_cols: Columns to be indexed.
        :return: Dataframe with indexed columns.
        """

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(input_cols, "input_cols")

        if isinstance(input_cols, str):
            input_cols = [input_cols]

        from pyspark.ml import Pipeline
        from pyspark.ml.feature import IndexToString

        indexers = [IndexToString(inputCol=column, outputCol=column + "_string") for column in
                    list(set(input_cols))]

        pipeline = Pipeline(stages=indexers)
        self._df = pipeline.fit(self._df).transform(self._df)

        return self

    def one_hot_encoder(self, input_cols):
        """
        Maps a column of label indices to a column of binary vectors, with at most a single one-value.
        :param input_cols: Columns to be encoded.
        :return: Dataframe with encoded columns.
        """

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(input_cols, "input_cols")

        if isinstance(input_cols, str):
            input_cols = [input_cols]

        from pyspark.ml import Pipeline
        from pyspark.ml.feature import OneHotEncoder

        encode = [OneHotEncoder(inputCol=column, outputCol=column + "_encoded") for column in
                  list(set(input_cols))]

        pipeline = Pipeline(stages=encode)
        self._df = pipeline.fit(self._df).transform(self._df)

        return self

    def sql(self, sql_expression):
        """
        Implements the transformations which are defined by SQL statement. Currently we only support
        SQL syntax like "SELECT ... FROM __THIS__ ..." where "__THIS__" represents the
        underlying table of the input dataframe.
        :param sql_expression: SQL expression.
        :return: Dataframe with columns changed by SQL statement.
        """

        self._assert_type_str(sql_expression, "sql_expression")

        from pyspark.ml.feature import SQLTransformer

        sql_trans = SQLTransformer(statement=sql_expression)

        self._df = sql_trans.transform(self._df)

        return self

    def vector_assembler(self, input_cols):
        """
        Combines a given list of columns into a single vector column.
        :param input_cols: Columns to be assembled.
        :return: Dataframe with assembled column.
        """

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(input_cols, "input_cols")

        if isinstance(input_cols, str):
            input_cols = [input_cols]

        from pyspark.ml import Pipeline

        assembler = [VectorAssembler(inputCols=input_cols, outputCol="features")]

        pipeline = Pipeline(stages=assembler)
        self._df = pipeline.fit(self._df).transform(self._df)

        return self

    def normalizer(self, input_cols, p=2.0):
        """
        Transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It takes parameter p, which
        specifies the p-norm used for normalization. (p=2) by default.
        :param input_cols: Columns to be normalized.
        :param p:  p-norm used for normalization.
        :return: Dataframe with normalized columns.
        """

        # Check if columns argument must be a string or list datatype:
        self._assert_type_str_or_list(input_cols, "input_cols")

        if isinstance(input_cols, str):
            input_cols = [input_cols]

        assert isinstance(p, (float, int)), "Error: p argument must be a numeric value."

        from pyspark.ml import Pipeline
        from pyspark.ml.feature import Normalizer

        normal = [Normalizer(inputCol=column, outputCol=column + "_normalized", p=p) for column in
                  list(set(input_cols))]

        pipeline = Pipeline(stages=normal)
        self._df = pipeline.fit(self._df).transform(self._df)

        return self
