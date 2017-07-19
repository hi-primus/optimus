# Importing sql types
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, StructType, StructField, ArrayType
# Importing sql functions
from pyspark.sql.functions import col, udf, trim, lit, format_number, months_between, date_format, unix_timestamp, \
    current_date, abs as mag
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
import re
import string
import unicodedata
import pyspark.sql.dataframe


class DataFrameTransformer():
    """DataFrameTransformer is a class to make transformations in dataFrames"""

    def __init__(self, df):
        """Class constructor.
        :param  df      DataFrame to be transformed.
        """
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), \
            "Error, df argument must be a pyspark.sql.dataframe.DataFrame instance"

        # Dataframe
        self.__df = df
        # SparkContext:
        # self.__sqlContext = SQLContext(self.__df.sql_ctx)
        self.__sqlContext = self.__df.sql_ctx
        self.__numberOfTransformations = 0

    def __assertTypeStrOrList(self, variable, nameArg):
        """This function asserts if variable is a string or a list dataType."""
        assert (type(variable) == type('str') or type(variable) == type([])), \
            "Error: %s argument must be a string or a list." % nameArg

    def __assertTypeStr(self, variable, nameArg):
        """This function asserts if variable is a string or a list dataType."""
        assert (type(variable) == type('str')), \
            "Error: %s argument must be a string." % nameArg

    def __assertColsInDF(self, columnsProvided, columnsDF):
        """This function asserts if columnsProvided exists in dataFrame.
        Inputs:
        columnsProvided: the list of columns to be process.
        columnsDF: list of columns's dataFrames
        """
        colNotValids = (set([column for column in columnsProvided]).difference(set([column for column in columnsDF])))
        assert (colNotValids == set()), 'Error: The following columns do not exits in dataFrame: %s' % colNotValids

    def __addTransformation(self):
        self.__numberOfTransformations += 1

        if (self.__numberOfTransformations > 50):
            self.checkPoint()
            self.__numberOfTransformations = 0

    def setDataframe(self, df):
        """This function set a dataframe into the class for subsequent actions.
        """
        assert isinstance(df, pyspark.sql.dataframe.DataFrame), "Error: df argument must a sql.dataframe type"
        self.__df = df

    def getDataframe(self):
        """This function return the dataframe of the class"""
        return self.__df

    def lowerCase(self, columns):
        """This function set all strings in columns of dataframe specified to lowercase.
        Columns argument must be a string or a list of string. In order to apply this function to all
        dataframe, columns must be equal to '*'"""

        func = lambda cell: cell.lower() if cell is not None else cell
        self.setCol(columns, func, 'string')
        return self

    def upperCase(self, columns):
        """This function set all strings in columns of dataframe specified to uppercase.
        Columns argument must be a string or a list of string. In order to apply this function to all
        dataframe, columns must be equal to '*'"""
        func = lambda cell: cell.upper() if cell is not None else cell
        self.setCol(columns, func, 'string')
        return self

    def checkPoint(self):
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
        print ("Saving changes at disk by checkpoint...")
        self.__df.rdd.checkpoint()
        self.__df.rdd.count()
        self.__df = self.__sqlContext.createDataFrame(self.__df.rdd, self.__df.schema)
        print ("Done.")

    execute = checkPoint

    def trimCol(self, columns):
        """This methods cut left and right extra spaces in column strings provided by user.
        :param columns   list of column names of dataFrame.
                If a string "*" is provided, the method will do the trimming operation in whole dataFrame.

        :return transformer object
        """

        # Function to trim spaces in columns with strings datatype
        def colTrim(columns):
            exprs = [trim(col(c)).alias(c)
                     if (c in columns) and (c in validCols)
                     else c
                     for (c, t) in self.__df.dtypes]
            self.__df = self.__df.select(*exprs)

        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        # Filters all string columns in dataFrame
        validCols = [c for (c, t) in filter(lambda t: t[1] == 'string', self.__df.dtypes)]

        # If None or [] is provided with column parameter:
        if (columns == "*"): columns = validCols

        # Columns
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        # Trimming spaces in columns:
        colTrim(columns)

        self.__addTransformation()

        # Returning the transformer object for able chaining operations
        return self

    def dropCol(self, columns):
        """This method eliminate the list of columns provided by user.
        :param columns      list of columns names or a string (a column name).

        :return transformer object
        """

        def colDrop(columns):
            exprs = filter(lambda c: c not in columns, self.__df.columns)
            self.__df = self.__df.select(*exprs)

        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        # Columns
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        # Calling colDrop function
        colDrop(columns)

        self.__addTransformation()

        # Returning the transformer object for able chaining operations
        return self

    def replaceCol(self, search, changeTo, columns):
        """This method search the 'search' value in DataFrame columns specified in 'columns' in order to replace it
        for 'changeTo' value.


        :param search       value to search in dataFrame.
        :param changeTo     value used to replace the old one in dataFrame.
        :param columns      list of string column names or a string (column name). If columns = '*' is provided,
                            searching and replacing action is made in all columns of DataFrame that have same
                            dataType of search and changeTo.

        search and changeTo arguments are expected to be numbers and same dataType ('integer', 'string', etc) each other.
        olumns argument is expected to be a string or list of string column names.

        :return transformer object
        """

        def colReplace(columns):
            self.__df = self.__df.replace(search, changeTo, subset=columns)

        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        # Asserting search parameter is a string or a number
        assert isinstance(search, str) or isinstance(search, float) or isinstance(search,
                                                                                  int), "Error: Search parameter must be a number or string"

        # Asserting changeTo parameter is a string or a number
        assert isinstance(changeTo, str) or isinstance(changeTo, float) or isinstance(changeTo,
                                                                                      int), "Error: changeTo parameter must be a number or string"

        # Asserting search and changeTo have same type
        assert type(search) == type(
            changeTo), 'Error: Search and ChangeTo must have same datatype: Integer, String, Float'

        # Change
        types = {type(''): 'string', type(int(1)): 'int', type(float(1.2)): 'float', type(1.2): 'double'};

        validCols = [c for (c, t) in filter(lambda t: t[1] == types[type(search)], self.__df.dtypes)]

        # If None or [] is provided with column parameter:
        if (columns == "*"): columns = validCols[:]

        # If columns is string, make a list:
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        colNotValids = (set([column for column in columns]).difference(set([column for column in validCols])))

        assert (
            colNotValids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % colNotValids

        colReplace(columns)

        self.__addTransformation()

        # Returning the transformer object for able chaining operations
        return self

    def deleteRow(self, func):
        """This function is an alias of filter and where spark functions.
        :param func     func must be an expression with the following form:

                func = col('colName') > value.

                func is an expression where col is a pyspark.sql.function.
        """
        self.__df = self.__df.filter(func)

        self.__addTransformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def setCol(self, columns, func, dataType):
        """This method can be used to make math operations or string manipulations in row of dataFrame columns.

        :param columns      list of columns (or a single column) of dataFrame.
        :param func         function or string type which describe the dataType that func function should return.
        :param dataType     string indicating one of the following options: 'integer', 'string', 'double','float'.

        'columns' argument is expected to be a string or a list of columns names.
        It is a requirement for this method that the dataType provided must be the same to dataType of columns.
        On the other hand, if user writes columns == '*' the method makes operations in func if only if columns
        have same dataType that dataType argument.

        :return transformer object
        """
        dictTypes = {'string': StringType(), 'str': StringType(), 'integer': IntegerType(),
                     'int': IntegerType(), 'float': FloatType(), 'double': DoubleType(), 'Double': DoubleType()}

        Types = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int',
                 'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}

        try:
            function = udf(func, dictTypes[dataType])
        except KeyError:
            assert False, "Error, dataType not recognized"

        def colSet(columns, function):
            exprs = [function(col(c)).alias(c) if c in columns else c for (c, t) in self.__df.dtypes]
            try:
                self.__df = self.__df.select(*exprs)
            except:
                assert False, "Error: Make sure if operation is compatible with row datatype."

        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        # Filters all string columns in dataFrame
        validCols = [c for (c, t) in filter(lambda t: t[1] == Types[dataType], self.__df.dtypes)]

        # If None or [] is provided with column parameter:
        if (columns == "*"): columns = validCols[:]

        # If columns is string, make a list:
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        colNotValids = (set([column for column in columns]).difference(set([column for column in validCols])))

        assert (
            colNotValids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % colNotValids

        colSet(columns, function)

        self.__addTransformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    # Drop
    def keepCol(self, columns):
        """This method keep only columns specified by user with columns argument in DataFrame.
        :param columns list of columns or a string (column name).

        :return transformer object
        """

        def colKeep(columns):
            exprs = filter(lambda c: c in columns, self.__df.columns)
            self.__df = self.__df.select(*exprs)

        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        # Check is column if a string.
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        # Calling colDrop function
        colKeep(columns)

        self.__addTransformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def clearAccents(self, columns):
        """This function deletes accents in strings column dataFrames, it does not eliminate main characters,
        but only deletes special tildes.

        :param columns  String or a list of column names.

        """

        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        # Filters all string columns in dataFrame
        validCols = [c for (c, t) in filter(lambda t: t[1] == 'string', self.__df.dtypes)]

        # If None or [] is provided with column parameter:
        if (columns == "*"): columns = validCols[:]

        # If columns is string, make a list:
        if isinstance(columns, str): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        colNotValids = (set([column for column in columns]).difference(set([column for column in validCols])))

        assert (
            colNotValids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % colNotValids

        # Receives  a string as an argument
        def remove_accents(inputStr):
            # first, normalize strings:
            nfkdStr = unicodedata.normalize('NFKD', inputStr)
            # Keep chars that has no other char combined (i.e. accents chars)
            withOutAccents = u"".join([c for c in nfkdStr if not unicodedata.combining(c)])
            return withOutAccents

        function = udf(lambda x: remove_accents(x) if x != None else x, StringType())
        exprs = [function(col(c)).alias(c) if (c in columns) and (c in validCols) else c for c in self.__df.columns]
        self.__df = self.__df.select(*exprs)

        self.__addTransformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def removeSpecialChars(self, columns):
        """This function remove special chars in string columns, such as: .!"#$%&/()
        :param columns      list of names columns to be processed.

        columns argument can be a string or a list of strings."""

        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        # Filters all string columns in dataFrame
        validCols = [c for (c, t) in filter(lambda t: t[1] == 'string', self.__df.dtypes)]

        # If None or [] is provided with column parameter:
        if (columns == "*"): columns = validCols[:]

        # If columns is string, make a list:
        if type(columns) == type(' '): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        colNotValids = (set([column for column in columns]).difference(set([column for column in validCols])))

        assert (
            colNotValids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % colNotValids

        def rmSpecChars(inputStr):
            # Remove all punctuation and control characters
            for punct in (set(inputStr) & set(string.punctuation)):
                inputStr = inputStr.replace(punct, "")
            return inputStr

        # User define function that does operation in cells
        function = udf(lambda cell: rmSpecChars(cell) if cell != None else cell, StringType())

        exprs = [function(c).alias(c) if (c in columns) and (c in validCols)  else c for c in self.__df.columns]

        self.__df = self.__df.select(*exprs)

        self.__addTransformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def removeSpecialCharsRegex(self, columns,regex):
        """This function remove special chars in string columns using a regex, such as: .!"#$%&/()
        :param columns      list of names columns to be processed.
        :param regex        string that contains the regular expression

        columns argument can be a string or a list of strings."""

        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        # Filters all string columns in dataFrame
        validCols = [c for (c, t) in filter(lambda t: t[1] == 'string', self.__df.dtypes)]

        # If None or [] is provided with column parameter:
        if (columns == "*"): columns = validCols[:]

        # If columns is string, make a list:
        if type(columns) == type(' '): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        colNotValids = (set([column for column in columns]).difference(set([column for column in validCols])))

        assert (
            colNotValids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % colNotValids

        def rmSpecCharsRegex(inputStr,regex):
            for s in set(inputStr):
                inputStr = re.sub(regex, '', inputStr)
            return inputStr

        # User define function that does operation in cells
        function = udf(lambda cell: rmSpecCharsRegex(cell,regex) if cell != None else cell, StringType())

        exprs = [function(c).alias(c) if (c in columns) and (c in validCols)  else c for c in self.__df.columns]

        self.__df = self.__df.select(*exprs)

        self.__addTransformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self

    def renameCol(self, columns):
        """"This functions change the name of columns datraFrame.
        :param columns      List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).

        """
        # Asserting columns is string or list:
        assert (type(columns) == type([])) and (
            type(columns[0]) == type((1, 2))), "Error: Column argument must be a list of tuples"

        colNotValids = (
            set([column[0] for column in columns]).difference(set([column for column in self.__df.columns])))

        assert (colNotValids == set()), 'Error: The following columns do not exits in dataFrame: %s' % colNotValids

        oldNames = [column[0] for column in columns]

        notInType = filter(lambda c: c not in oldNames, self.__df.columns)

        exprs = [col(column[0]).alias(column[1]) for column in columns] + [col(column) for column in notInType]

        self.__addTransformation()  # checkpoint in case

        self.__df = self.__df.select(*exprs)
        return self

    def lookup(self, column, strToReplace, listStr=None):
        """This method search a list of strings specified in `listStr` argument among rows
        in column dataFrame and replace them for `StrToReplace`.

        :param  column      Column name, this variable must be string dataType.
        :param  strToReplace    string that going to replace all others present in listStr argument
        :param  listStr     List of strings to be replaced

        `lookup` can only be runned in StringType columns.


        """
        # Check if columns argument a string datatype:
        self.__assertTypeStr(column, "column")

        # Asserting columns is string or list:
        assert type(strToReplace) == type('') or (
            type(strToReplace) == type({})), "Error: StrToReplace argument must be a string or a dict"

        if type(strToReplace) == type({}):
            assert (strToReplace != {}), "Error, StrToReplace must be a string or a non empty python dictionary"
            assert (
                listStr == None), "Error, If a python dictionary if specified, listStr argument must be None: listStr=None"

        # Asserting columns is string or list:
        assert (type(listStr) == type([]) and listStr != []) or (
            listStr == None), "Error: Column argument must be a non empty list"

        if type(strToReplace) == type(''):
            assert listStr != None, "Error: listStr cannot be None if StrToReplace is a String, please you need to specify \
             the listStr string"

        # Filters all string columns in dataFrame
        validCols = [c for (c, t) in filter(lambda t: t[1] == 'string', self.__df.dtypes)]

        if type(column) == type('str'): column = [column]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=column, columnsDF=self.__df.columns)

        # Asserting if selected column datatype and search and changeTo parameters are the same:
        colNotValids = (set(column).difference(set([column for column in validCols])))
        assert (colNotValids == set()), 'Error: The column provided is not a column string: %s' % colNotValids

        # User defined function to search cell value in list provide by user:
        if type(strToReplace) == type('str') and listStr is not None:

            def revisar(cell):
                if cell is not None and (cell in listStr):
                    return strToReplace
                else:
                    return cell

            func = udf(lambda cell: revisar(cell), StringType())
        else:
            def replaceFromDic(strTest):
                for key in strToReplace.keys():
                    if strTest in strToReplace[key]:
                        strTest = key
                return strTest

            func = udf(lambda cell: replaceFromDic(cell) if cell != None else cell, StringType())

        # Calling udf for each row of column provided by user. The rest of dataFrame is
        # maintained the same.
        exprs = [func(col(c)).alias(c) if c == column[0] else c for c in self.__df.columns]

        self.__df = self.__df.select(*exprs)

        self.__addTransformation()  # checkpoint in case

        return self

    def moveCol(self, column, refCol, position):
        """This funcion change column position in dataFrame.
        :param column   Name of the column to be moved in dataFrame. column argument must be a string.
        :param refCol   Name of reference column in dataFrame. This column will be a reference to place the
                        column to be moved.
        :param position Can be one of the following options: 'after' or 'before'. If 'after' is provided, column
                        provided will be placed just after the refCol selected."""
        # Columns of dataFrame
        columns = self.__df.columns

        # Check if columns argument a string datatype:
        self.__assertTypeStr(column, "column")

        # Check if column to be process are in dataframe
        self.__assertColsInDF(columnsProvided=[column], columnsDF=self.__df.columns)

        # Check if columns argument a string datatype:
        self.__assertTypeStr(refCol, "refCol")

        # Asserting parameters are not empty strings:
        assert (
            (column != '') and (refCol != '') and (position != '')), "Error: Input parameters can't be empty strings"

        # Check if refCol is in dataframe
        self.__assertColsInDF(columnsProvided=[refCol], columnsDF=self.__df.columns)

        # Check if columns argument a position string datatype:
        self.__assertTypeStr(position, "position")

        # Asserting if position is 'after' or 'before'
        assert (position == 'after') or (
            position == 'before'), "Error: Position parameter only can be 'after' or 'before'"

        # Finding position of column to move:
        findCol = lambda columns, column: [index for index, c in enumerate(columns) if c == column]
        newIndex = findCol(columns, refCol)
        oldIndex = findCol(columns, column)

        # if position is 'after':
        if position == 'after':
            # Check if the movement is from right to left:
            if newIndex[0] >= oldIndex[0]:
                columns.insert(newIndex[0], columns.pop(oldIndex[0]))  # insert and delete a element
            else:  # the movement is form left to right:
                columns.insert(newIndex[0] + 1, columns.pop(oldIndex[0]))
        else:  # If position if before:
            if newIndex[0] >= oldIndex[0]:  # Check if the movement if from right to left:
                columns.insert(newIndex[0] - 1, columns.pop(oldIndex[0]))
            elif newIndex[0] < oldIndex[0]:  # Check if the movement if from left to right:
                columns.insert(newIndex[0], columns.pop(oldIndex[0]))

        self.__df = self.__df[columns]

        self.__addTransformation()  # checkpoint in case

        return self

    def explodeTable(self, colId, col1, newColFeature, listToAssign):
        """
        This function can be used to split a feature with some extra information in order
        to make a new column feature.

        :param colId    column name of the columnId of dataFrame
        :param col1     column name of the column to be split.
        :param newColFeature        Name of the new column.
        :param listToAssign         List of values to be counted.

        Please, see documentation for more explanations about this method.

        """
        # Asserting if position is string or list:

        assert type(listToAssign) == type([]), "Error: listToAssign argument must be a list"

        # Asserting parameters are not empty strings:
        assert (
            (colId != '') and (col1 != '') and (newColFeature != '')), "Error: Input parameters can't be empty strings"

        # Check if col1 argument is string datatype:
        self.__assertTypeStr(col1, "col1")

        # Check if newColFeature argument is a string datatype:
        self.__assertTypeStr(newColFeature, "newColFeature")

        # Check if colId argument is a string datatype:
        self.__assertTypeStr(colId, "colId")

        # Check if colId to be process are in dataframe
        self.__assertColsInDF(columnsProvided=[colId], columnsDF=self.__df.columns)

        # Check if col1 to be process are in dataframe
        self.__assertColsInDF(columnsProvided=[col1], columnsDF=self.__df.columns)

        # subset, only PAQ and Tipo_Unidad:
        subdf = self.__df.select(colId, col1)

        # dataframe Filtered:
        dfMod = self.__df.where(self.__df[col1] != newColFeature)

        # subset de
        newColumn = subdf.where(subdf[col1] == newColFeature).groupBy(colId).count()

        # Left join:
        newColumn = newColumn.withColumnRenamed(colId, colId + '_other')

        for x in range(len(listToAssign)):
            if x == 0:
                exprs = (dfMod[colId] == newColumn[colId + '_other']) & (dfMod[col1] == listToAssign[x])
            else:
                exprs = exprs | (dfMod[colId] == newColumn[colId + '_other']) & (dfMod[col1] == listToAssign[x])

        dfMod = dfMod.join(newColumn, exprs, 'left_outer')

        # Cleaning dataframe:
        dfMod = dfMod.drop(colId + '_other').na.fill(0).withColumnRenamed('count', newColFeature)
        self.__df = dfMod

        self.__addTransformation()  # checkpoint in case

        return self

    def dateTransform(self, columns, currentFormat, outputFormat):
        """
        :param  columns     Name date columns to be transformed. Columns ha
        :param  currentFormat   currentFormat is the current string dat format of columns specified. Of course,
                                all columns specified must have the same format. Otherwise the function is going
                                to return tons of null values because the transformations in the columns with
                                different formats will fail.
        :param  outputFormat    output date string format to be expected.
        """
        # Check if currentFormat argument a string datatype:
        self.__assertTypeStr(currentFormat, "currentFormat")
        # Check if outputFormat argument a string datatype:
        self.__assertTypeStr(outputFormat, "outputFormat")
        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        if type(columns) == type('str'): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        exprs = [date_format(unix_timestamp(c, currentFormat).cast("timestamp"), outputFormat).alias(
            c) if c in columns else c for c in self.__df.columns]

        self.__df = self.__df.select(*exprs)

        self.__addTransformation()  # checkpoint in case

        return self

    def ageCalculate(self, column, dateFormat, nameColAge):
        """
        This method compute the age of clients based on their born dates.
        :param  column      Name of the column born dates column.
        :param  dateFormat  String format date of the column provided.
        :param  nameColAge  Name of the new column, the new columns is the resulting column of ages.

        """
        # Check if column argument a string datatype:
        self.__assertTypeStr(column, "column")

        # Check if dateFormat argument a string datatype:
        self.__assertTypeStr(dateFormat, "dateFormat")

        # Asserting if column if in dataFrame:
        assert column in self.__df.columns, "Error: Column assigned in column argument does not exist in dataFrame"

        # Output format date
        Format = "yyyy-MM-dd"  # Some SimpleDateFormat string

        exprs = format_number(
            mag(
                months_between(date_format(
                    unix_timestamp(column, dateFormat).cast("timestamp"), Format), current_date()) / 12), 4).alias(
            nameColAge)

        self.__df = self.__df.withColumn(nameColAge, exprs)

        self.__addTransformation()  # checkpoint in case

        return self

    def castFunc(self, colsAndTypes):
        """

        :param colsAndTypes     List of tuples of column names and types to be casted. This variable should have the
                                following structure:

                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]

                The first parameter in each tuple is the column name, the second is the finale datatype of column after
                the transformation is made.
        :return:
        """

        dictTypes = {'string': StringType(), 'str': StringType(), 'integer': IntegerType(),
                     'int': IntegerType(), 'float': FloatType(), 'double': DoubleType(), 'Double': DoubleType()}

        Types = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int',
                 'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}

        # Asserting colsAndTypes is string or list:
        assert type(colsAndTypes) == type('s') or type(colsAndTypes) == type(
            []), "Error: Column argument must be a string or a list."

        if type(colsAndTypes) == type(''): colsAndTypes = [colsAndTypes]

        columnNames = [column[0] for column in colsAndTypes]

        # Check if columnNames to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columnNames, columnsDF=self.__df.columns)

        notSpecifiedColumns = filter(lambda c: c not in columnNames, self.__df.columns)

        exprs = [col(column[0]).cast(dictTypes[Types[column[1]]]).alias(column[0]) for column in colsAndTypes] + [
            col(column) for column in notSpecifiedColumns]

        self.__df = self.__df.select(*exprs)
        self.__addTransformation()  # checkpoint in case

        return self

    def emptyStrToStr(self, columns, customStr):
        """
        This function replace a string specified 
        """
        # Check if customStr argument a string datatype:
        self.__assertTypeStr(customStr, "customStr")

        # Check if columns argument must be a string or list datatype:
        self.__assertTypeStrOrList(columns, "columns")

        # Filters all string columns in dataFrame
        validCols = [c for (c, t) in filter(lambda t: t[1] == 'string', self.__df.dtypes)]

        # If None or [] is provided with column parameter:
        if (columns == "*"): columns = validCols[:]

        # If columns is string, make a list:
        if type(columns) == type(' '): columns = [columns]

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        def blank_as_null(x):
            return when(col(x) != "", col(x)).otherwise(customStr)

        exprs = [blank_as_null(c).alias(c) if (c in columns) and (c in validCols)  else c for c in self.__df.columns]

        self.__df = self.__df.select(*exprs)
        self.__addTransformation()  # checkpoint in case

        return self

    def operationInType(self, parameters):
        """ This function makes operations in a columnType of dataframe. It is well know that DataFrames are consistent,
        but it in this context, operation are based in types recognized by the dataframe analyzer, types are identified
        according if the value is parsable to int or float, etc.

        This functions makes the operation in column elements that are recognized as the same type that the dataType
        argument provided in the input function.

        Columns provided in list of tuples cannot be repeated
        :param parameters   List of columns in the following form: [(columnName, dataType, func),
                                                                    (columnName1, dataType1, func1)]
        :return None
        """

        def checkDataType(value):

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

        Types = {type('str'): 'string', type(1): 'int', type(1.0): 'float'}

        exprs = []
        for column, dataType, func in parameters:
            # Cheking if column name is string datatype:
            self.__assertTypeStr(column, "columnName")
            # Checking if column exists in dataframe:
            assert column in self.__df.columns, "Error: Column %s specified as columnName argument does not exist in dataframe" % column
            # Checking if column has a valid datatype:
            assert (dataType in ['integer', 'float', 'string',
                                 'null']), "Error: dataType only can be one of the followings options: integer, float, string, null."
            # Checking if func parameters is func dataType or None
            assert (type(func) == type(None) or (
                type(func) == type(lambda x: x))), "func argument must be a function or NoneType"

            if 'function' in str(type(func)):
                funcUdf = udf(lambda x: func(x) if checkDataType(x) == dataType else x)

            if isinstance(func, str) or isinstance(func, int) or isinstance(func, float):
                assert [x[1] in Types[type(func)] for x in filter(lambda x: x[0] == columnName, self.__df.dtypes)][
                    0], "Error: Column of operation and func argument must be the same global type. Check column type by df.printSchema()"
                funcUdf = udf(lambda x: func if checkDataType(x) == dataType else x)

            if func is None:
                funcUdf = udf(lambda x: None if checkDataType(x) == dataType else x)

            exprs.append(funcUdf(col(column)).alias(column))

        colNotProvided = [x for x in self.__df.columns if x not in [column[0] for column in parameters]]

        self.__df = self.__df.select(colNotProvided + [*exprs])
        self.__addTransformation()  # checkpoint in case

        return self

    def rowFilterByType(self, columnName, typeToDelete):
        """This function has built in order to deleted some type of dataframe """
        # Check if columnName argument a string datatype:
        self.__assertTypeStr(columnName, "columnName")
        # Asserting if columnName exits in dataframe:
        assert columnName in self.__df.columns, "Error: Column specified as columnName argument does not exist in dataframe"
        # Check if typeToDelete argument a string datatype:
        self.__assertTypeStr(typeToDelete, "typeToDelete")
        # Asserting if dataType argument has a valid type:
        assert (typeToDelete in ['integer', 'float', 'string',
                                 'null']), "Error: dataType only can be one of the followings options: integer, float, string, null."

        # Function for determine if register value is float or int or string:
        def dataType(value):

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

        func = udf(dataType, StringType())
        self.__df = self.__df.withColumn('types', func(col(columnName))).where((col('types') != typeToDelete)).drop(
            'types')
        self.__addTransformation()  # checkpoint in case

        return self

    def undoVecAssembler(self, column, featureNames):
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
        self.__assertTypeStr(column, "column")

        assert (column in self.__df.columns), "Error: column specified does not exist in dataFrame."

        assert (type(featureNames) == type([])), "Error: featureNames must be a list of strings."
        # Function to extract value from list column:
        func = udf(lambda x, index: x[index])

        exprs = []

        # Recursive function:
        def exprsFunc(column, exprs, featureNames, index):
            if index == 0:
                return [func(col(column), lit(index)).alias(featureNames[index])]
            else:
                return exprsFunc(column, exprs, featureNames, index - 1) + [
                    func(col(column), lit(index)).alias(featureNames[index])]

        self.__df = self.__df.select(
            [x for x in self.__df.columns] + [*exprsFunc(column, exprs, featureNames, len(featureNames) - 1)]).drop(
            column)
        self.__addTransformation()  # checkpoint in case

        return self

    def scaleVecCol(self, columns, nameOutputCol):
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
        self.__assertTypeStrOrList(columns, "columns")

        # Check if columns to be process are in dataframe
        self.__assertColsInDF(columnsProvided=columns, columnsDF=self.__df.columns)

        # Check if nameOutputCol argument a string datatype:
        self.__assertTypeStr(nameOutputCol, "nameOutpuCol")

        # Model to use vectorAssember:
        vecAssembler = VectorAssembler(inputCols=columns, outputCol="features_assembler")
        # Model for scaling feature column:
        mmScaler = MinMaxScaler(inputCol="features_assembler", outputCol=nameOutputCol)
        # Dataframe with feature_assembler column
        tempDF = vecAssembler.transform(self.__df)
        # Fitting scaler model with transformed dataframe
        model = mmScaler.fit(tempDF)

        exprs = list(filter(lambda x: x not in columns, self.__df.columns))

        exprs.extend([nameOutputCol])

        self.__df = model.transform(tempDF).select(*exprs)
        self.__addTransformation()  # checkpoint in case

        return self

    def splitStrCol(self, column, featureNames, mark):
        """This functions split a column into different ones. In the case of this method, the column provided should
        be a string of the following form 'word,foo'.

        :param column       Name of the target column, this column is going to be replaced.
        :param featureNames     List of strings of the new column names after splitting the strings.
        :param mark         String that specifies the splitting mark of the string, this frequently is ',' or ';'.
        """

        # Check if column argument is a string datatype:
        self.__assertTypeStr(column, "column")

        # Check if mark argument is a string datatype:
        self.__assertTypeStr(mark, "mark")

        assert (column in self.__df.columns), "Error: column specified does not exist in dataFrame."

        assert (type(featureNames) == type([])), "Error: featureNames must be a list of strings."

        # Setting a udf that split the string into a list of strings.
        # This is "word, foo" ----> ["word", "foo"]
        func = udf(lambda x: x.split(mark), ArrayType(StringType()))

        self.__df = self.__df.withColumn(column, func(col(column)))
        self.undoVecAssembler(column=column, featureNames=featureNames)
        self.__addTransformation()  # checkpoint in case

        return self

    def writeDFAsJson(self, path):
        p = re.sub("}\'", "}", re.sub("\'{", "{", str(self.__df.toJSON().collect())))

        with open(path, 'w') as outfile:
            # outfile.write(str(jsonCols).replace("'", "\""))
            outfile.write(p)


if __name__ == "__main__":
    pass

