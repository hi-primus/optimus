# -*- coding: utf-8 -*-
# Importing sql functions
from pyspark.sql.functions import udf, when, col, min as cmin, max as cmax
from pyspark.sql.types import StringType, DoubleType
# Importing plotting libraries
import matplotlib.pyplot as plt
# Importing numeric module
import numpy as np
# Importing features to build tables
from IPython.display import display, HTML
# Importing time librarie to measure time
import time
# Importing dumps
from json import dumps
import pyspark.sql.dataframe

plt.style.use('ggplot')

class ColumnTables():
    """This class builds a table to describe the number of the different dataTypes in a column dataFrame.

    It is important to notice that this is not the best way to build a table. It will be better if a general
    building table class is built"""
    def __init__(self, colName, dataTypeInferred, qtys, percents):
        self.qtys = qtys
        self.percents = percents
        self.colName = colName
        self.dataTypeInferred = dataTypeInferred
        self.labelsTable = ["None", "Empty str", "String", "Integer", "Float"]

    def _repr_html_(self):
        # Creation of blank table:
        html = ["<table width=50%>"]

        # Adding a row:
        html.append("<tr>")
        html.append("<td colspan=3 >{0}</td>".format("<b> Column name: </b>" + self.colName))
        html.append("</tr>")

        # Adding a row:
        html.append("<tr>")
        html.append("<td colspan=3 >{0}</td>".format("<b> Column datatype: </b>" + self.dataTypeInferred))
        html.append("</tr>")

        # Adding a row:
        html.append("<tr>")
        html.append("<th>{0}</td>".format('Datatype'))
        html.append("<th>{0}</td>".format('Quantity'))
        html.append("<th>{0}</td>".format('Percentage'))
        html.append("</tr>")

        for ind in range(len(self.labelsTable)):
            html.append("<tr>")
            html.append("<td>{0}</td>".format(self.labelsTable[ind]))
            html.append("<td>{0}</td>".format(self.qtys[ind + 1]))
            html.append("<td>{0}</td>".format("%0.2f" % self.percents[ind] + " %"))
            html.append("</tr>")

        html.append("</table>")
        return ''.join(html)


# Input data:
class GeneralDescripTable():
    def __init__(self, fileName, columnNumber, rowNumber):
        self.labels = ['File Name', "Columns", "Rows"]
        self.dat = [fileName, columnNumber, rowNumber]

    def _repr_html_(self):
        # Creation of blank table:
        html = ["<table width=50%>"]
        # Adding a row:
        html.append("<tr>")
        html.append("<th colspan=3>{0}</td>".format('General description'))
        html.append("</tr>")

        # Adding a row:
        html.append("<tr>")
        html.append("<th colspan=1>{0}</td>".format('Features'))
        html.append("<th colspan=2>{0}</td>".format('Name or Quantity'))
        html.append("</tr>")

        for ind in range(len(self.dat)):
            # Adding a row:
            html.append("<tr>")
            html.append("<th colspan=1>{0}</td>".format(self.labels[ind]))
            html.append("<td colspan=2>{0}</td>".format(self.dat[ind]))
            html.append("</tr>")

        return ''.join(html)
        # self.body =  ''.join(html)


class DataTypeTable():
    def __init__(self, lista):
        self.lista = lista
        # self.tuples = tuples
        self.html = ""

    def colTable(self, lista):
        html = []
        html.append("<table width=100%>")
        for x in lista:
            html.append("<tr >")
            html.append("<td style='text-align: center'>")

            html.append("<div style='min-height: 20px;'>")
            html.append(str(x))
            html.append("</div>")
            html.append("</td>")
            html.append("</tr>")
        html.append("</table>")
        return "".join(html)

    def _repr_html_(self):
        self.html = ["<table width=50%>"]
        headers = ['integers', 'floats', 'strings']

        # This verify is some of list of types is empty.
        # The idea is not to draw an empty table.
        ver = [x != [] for x in self.lista]

        # First row of table:
        self.html.append("<tr>")
        for x in range(len(headers)):
            if ver[x]:  # If list is not empty, print its head
                self.html.append("<th style='text-align: center'>")
                self.html.append(str(headers[x]))
                self.html.append("</th>")
        self.html.append("</tr>")

        # Adding the second row:
        self.html.append("<tr>")
        for x in range(len(self.lista)):
            if ver[x]:
                # Adding a td
                self.html.append("<td style='vertical-align: top;text-align: center;'>")
                # Adding a table:


                self.html.append(self.colTable(self.lista[x]))

                self.html.append("</td>")
        self.html.append("</tr>")

        self.html.append("</table>")
        return ''.join(self.html)


# This class makes an analisis of dataframe datatypes and its different general features.
class DataFrameAnalizer():
    def __init__(self, df, pathFile, pu=0.1, seed=13):
        # Asserting if df is dataFrame datatype.
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), \
            "Error, df argument must be a pyspark.sql.dataframe.DataFrame instance"
        # Asserting if path specified is string datatype
        assert (isinstance(pathFile, str)), \
            "Error, pathFile argument must be string datatype."
        # Asserting if path includes the type of filesystem
        assert (("file:///" == pathFile[0:8]) or ("hdfs:///" == pathFile[0:8])), \
            "Error: path must be with a 'file://' prefix \
            if the file is in the local disk or a 'path://' \
            prefix if the file is in the Hadood file system"

        # Asserting if seed is integer
        assert isinstance(seed, int), "Error, seed must be an integer"

        # Asserting pu is between 0 and 1.
        assert (pu <= 1) and (pu >= 0), "Error, pu argument must be between 0 and 1"

        # Dataframe
        self.__rowNumber = 0
        self.__pathFile = pathFile
        self.__currentlyObserved = 0  # 0 => whole 1 => partition
        self.__df = df
        self.__df.cache()
        self.__sample = df.sample(False, pu, seed)

    def __createDict(self, keys, values):
        """This functions is a helper to build dictionaries. The first argument must be a list of keys but it
        can be a string also (in this case the string will be packaged into a list. The keys provided
        will be the dictionary keys. The second argument represents the values of each key. values argument can
        be a list or another dictionary."""

        dicc = {}
        # Assert if keys is a string, if not, the string is placed it inside a list
        if isinstance(keys, str) == True: keys = [keys]
        # If values is not a list, place it inside a list also
        if not type(values) == type([]): values = [values]
        # Making dictionary
        for index in range(len(keys)): dicc[keys[index]] = values[index]
        # Return dictionary built
        return dicc

    def __verification(self, tempDf, columnName):
        # Function for determine if register value is float or int or string:
        def dataType(value):
            if isinstance(value, int):  # Check if value is integer
                return 'integer'
            elif isinstance(value, float):
                return 'float'
            elif isinstance(value, str):
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
            else:
                return 'null'

        func = udf(dataType, StringType())
        tempDf = tempDf.withColumn('types', func(col(columnName)))

        types = tempDf.groupBy('types').count()

        typeslabels = ['integer', 'float', 'string', 'null']

        numbers = {}
        for row in types.collect(): numbers[row[0]] = row[1]

        # completing values of rest types:
        for label in typeslabels:
            if not label in numbers:
                numbers[label] = 0

        numberEmptyStrs = tempDf.where(col(columnName) == '').count()
        numbers['string'] = numbers['string'] - numberEmptyStrs

        # List of returning values:
        values = [tempDf, numbers['null'], numberEmptyStrs, numbers['string'], numbers['integer'], numbers['float']]
        return values

        # Analize of each column:
    def __analize(self, dfColAnalizer, column, rowNumber, plots, printType, valuesBar, numBars, typesDict):
        t = time.time()
        sampleTableDict = {'string': 0., 'integer': 0, 'float': 0}
        # Calling verification ruotine to obtain datatype's counts
        # returns: [dataframeColumn, number of nulls, number of strings, number of integers, number of floats]
        dtypeNumbers = self.__verification(dfColAnalizer.select(column), column)

        def validColCheck(f):
            """This functions analyze if column has more than two different data types
            and return True if the column analyzed is found to be with different datatypes"""
            # Verifying if there is a dummy column:
            dummy = any(x == 1 for x in f[0:2]) and all(x == 0 for x in f[2:])

            # Verifying if there is a column with different datatypes:
            # Sum the first and the second number:
            sumFirstAndSecond = lambda lista: [lista[x] + lista[0] if x == 1 else lista[x] for x in range(len(lista))][
                                              1:]
            # Check if the column has different datatypes:
            differentTypes = sum([1 if x == 0 else 0 for x in sumFirstAndSecond(f[1:])]) < 2

            return dummy or differentTypes

        # Calculate percentages of datatypes:
        percentages = [(dtypeNumber / float(rowNumber)) * 100 for dtypeNumber in dtypeNumbers[1:]]

        if (validColCheck(dtypeNumbers[1:])):
            invalidCols = column
        else:
            invalidCols = False

        # Instance of columnTables to display results:
        typeCol = dfColAnalizer.select(column).dtypes[0][1]

        tempObj = ColumnTables(column, typeCol, dtypeNumbers, percentages)
        display(tempObj)

        if typeCol != 'string':
            print("Min value: ", dfColAnalizer.select(cmin(col(column))).first()[0])
            print("Max value: ", dfColAnalizer.select(cmax(col(column))).first()[0])

        # Plot bar stack:
        # if plots==True: self.__bar_stack_type(percentages, column)

        if printType == True:
            typeslabels = ['integer', 'float', 'string']
            listOfEachtype = [dtypeNumbers[0].where(col('types') == tipo).where(col(column) != '') \
                                  .drop('types').select(column).distinct() \
                                  .limit(numBars).map(lambda x: x[column]).collect() for tipo in typeslabels]
            display(DataTypeTable(listOfEachtype))
            sampleTableDict = self.__createDict(typeslabels, listOfEachtype)

        # Obtaining number of strings and numbers to decide what type of histogram (numerical
        # or categorical is needed)
        stringQty = dtypeNumbers[3]
        numberQty = np.sum(dtypeNumbers[4:])

        # Plotting histograms:
        # If number of strings is greater than (number of integers + number of floats)
        if (stringQty > numberQty):
            # Building histogram:
            histDict = self.getCategoricalHist(dfColAnalizer.select(column), numBars)
            histPlot = {"data": histDict}

            if plots == True:
                self.plotHist(
                    dfOneCol=dfColAnalizer.select(column),
                    histDict=histDict,
                    typeHist='categorical',
                    numBars=numBars,
                    valuesBar=valuesBar)
                plt.show()


        elif (stringQty < numberQty):
            # Building histogram:
            histDict = self.getNumericalHist(dfColAnalizer.select(column), numBars)
            histPlot = {"data": histDict}

            if plots == True:
                # Create the general blog and the "subplots" i.e. the bars
                histPlot = self.plotHist(dfColAnalizer.select(column),
                                        histDict=histDict,
                                        typeHist='numerical',
                                        numBars=numBars,
                                        valuesBar=valuesBar)
                plt.show()

        else:
            print("No valid data to print histogram or plots argument set to False")

            histPlot = {"data": [{"count": 0, "value": "none"}]}




        numbers = list(dtypeNumbers[1:])
        valid_values = self.__createDict(['total', 'string', 'integer', 'float'],
                                         [int(np.sum(dtypeNumbers[-3:])),
                                          numbers[-3],
                                          numbers[-2],
                                          numbers[-1]]
                                         )

        missing_values = self.__createDict(
            ['total', 'empty', 'null'],
            [int(np.sum(numbers[0:2])), numbers[1], numbers[0]
             ])

        # returns: [number of nulls, number of strings, number of integers, number of floats]
        summaryDict = self.__createDict(
            ["name", "type", "total", "valid_values", "missing_values"],
            [column, typesDict[typeCol], rowNumber, valid_values, missing_values
             ])

        columnDict = self.__createDict(
            ["summary", "graph", "sample"],
            [summaryDict, histPlot, sampleTableDict]
        )

        print ("end of __analyze", time.time() - t)
        return invalidCols, percentages, numbers, columnDict


    # This function, place values of frequency in histogram bars.
    def __valuesOnBar(self, plotFig):
        rects = plotFig.patches
        for rect in rects:
            # Getting height of bars:
            height = rect.get_height()
            # Plotting texts on bars:
            plt.text(rect.get_x() + rect.get_width() / 2.,
                     1.001 * height, "%.2e" % int(height),
                     va='bottom', rotation=90)

    def __plotNumHist(self, histDict, column, valuesBar):
        values = [list(lista) for lista in list(zip(*[(dic['value'], dic['cont']) for dic in histDict]))]
        index = np.arange(len(values[0]))

        bins = values[0]

        if len(bins) == 1:
            width = bins[0] * 0.3
            bins[0] = bins[0] - width
        else:
            width = min(abs(np.diff(bins))) * 0.15

        # Plotting histogram:
        plotFig = plt.bar(np.array(bins) - width, values[1], width=width)

        if valuesBar == True: self.__valuesOnBar(plotFig)

        if len(bins) == 1:
            plt.xticks(np.round(bins))
            plt.xlim([0, bins[0] + 1])
        else:
            plt.xticks(np.round(bins))

        plt.ylim([0, np.max(values[1]) * 1.25])
        # Plot Title:
        plt.title(column)
        # Limits Y axes
        plt.show()

    def __plotCatHist(self, histDict, column, valuesBar):
        # Extracting values from dictionary
        k = list(filter(lambda k: k != 'cont', histDict[0].keys()))[0]

        values = [list(lista) for lista in list(zip(*[(dic[k], dic['cont']) for dic in histDict]))]
        index = np.arange(len(values[0]))

        # Plot settings
        fig, ax = plt.subplots()
        # We need to draw the canvas, otherwise the labels won't be positioned and
        # won't have values yet.
        fig.canvas.draw()
        # Setting values of xticks
        ax.set_xticks(index)
        # Setting labels to the ticks
        ax.set_xticklabels(values[0], rotation=90)

        # Plot of bars:
        width = 0.5
        bins = index - width / 2
        plotFig = plt.bar(bins, values[1], width=width)

        # If user want to see values of frequencies over each bar
        if valuesBar == True: self.__valuesOnBar(plotFig)

        # Plot Title:
        plt.title(column)
        # Limits Y axes
        plt.ylim([0, np.max(values[1]) * 1.3])
        plt.xlim([-1, index[-1] + 1])
        plt.show()

    def __swapStatus(self):
        self.__exchangeData()  # exchange data
        self.__currentlyObserved = 0 if self.__currentlyObserved == 1 else 1

    def __exchangeData(self):  # Swaps the data among DF and the Sample
        if self.__currentlyObserved == 0:
            self.__hiddenData = self.__df
            self.__df = self.__sample

        else:
            self.__df = self.__hiddenData

    def analyzeSample(self):
        if self.__currentlyObserved == 0:
            self.__swapStatus()

    def analyzeCompleteData(self):
        if self.__currentlyObserved == 1:
            self.__swapStatus()

    def unpersistDF(self):
        self.__df.unpersist()

    def setDataframe(self, df):
        """This function set a dataframe into the class for subsequent actions.
        """
        # assert isinstance(df, pyspark.sql.dataframe.DataFrame), "Error: df argument must a sql.dataframe type"
        self.__df = df

    def getDataframe(self):
        """This function return the dataframe of the class"""
        return self.__df

    # Function to give general features of dataFrame:
    def generalDescription(self):
        # General data description:
        # Filename:
        fileName = self.__pathFile.split('/')[-1]
        # Counting the number of columns:
        rowNumber = self.__df.count()
        # Counting the number of rows:
        colNumber = len(self.__df.columns)
        # Writting General Description table:
        tempObj = GeneralDescripTable(fileName, colNumber, rowNumber)
        display(tempObj)

        dataTypes = len(set([x[1] for x in self.__df.dtypes]))

        return self.__createDict(["filename", "size", "columns", "rows", "datatypes"],
                                 [fileName, 500, colNumber, rowNumber, dataTypes])

    # Funtion to analize column datatypes, it also plot proportion datatypes and histograms:
    def columnAnalize(self, columnList, plots=True, valuesBar=True, printType=False, numBars=10):
        """
        # This function counts the number of registers in a column that are numbers (integers, floats) and the number of
        # string registers.
        # Input:
        # columnList: a list or a string column name
        # valuesBar (optional): Can be True or False. If it is True, frequency values are placed over each bar.
        # printType (optional): Can be one of the following strings: 'integer', 'string', 'float'. Depending of what string
        # is provided, a list of distinct values of that type is printed.
        # numBars: number of bars printed in histogram
        # Output:
        # values: a list containing the number of the different datatypes [nulls, strings, integers, floats]
        """
        # Asserting data variable columnList:
        assert type(columnList) == type([1, 2]) or type(columnList) == type(' '), "Error: columnList has to be a list."

        # Asserting if valuesBar is type Boolean
        assert type(valuesBar) == type(True), "Error: valuesBar must be boolean, True or False."

        # Asserting if printType is "string", "integer" or "float"
        assert (type(printType) == type(True)), "Error: printType must be boolean. True or False."

        # Counting
        time1 = time.time()

        dfColAnalizer = self.__df

        # Column assignation:
        columns = []

        # If columnList is a string, convert it in a list:
        if type(columnList) == type(' '):
            if columnList == "*":
                columns = dfColAnalizer.columns
            else:
                columns = [columnList]

        else:
            if len(columnList) > 1:
                columns = columnList

        # Asserting if columns provided are in dataFrame:
        assert all(
            col in dfColAnalizer.columns for col in columns), 'Error: Columns or column does not exist in the dataFrame'

        types = {"string": "ABC", "int": "#", "integer": "#", "float": "##.#", "double": "##.#", "bigint": "#"}

        rowNumber = self.__df.count()

        invalidCols = []
        jsonCols = []
        # In case first is not set, this will analize all columns
        for column in columns:
            # Calling function analize:
            invCol, percentages, dtypeNumbers, d = self.__analize(
                dfColAnalizer.select(column),
                column,
                rowNumber,
                plots,
                printType,
                valuesBar=valuesBar,
                numBars=numBars,
                typesDict=types)

            # Save the invalid col if exists
            invalidCols.append(invCol)

            jsonCols.append(d)

        time2 = time.time()
        print("Total execution time: ", (time2 - time1))

        invalidCols = list(filter(lambda x: x != False, invalidCols))

        jsonCols = self.__createDict(["summary", "columns"], [self.generalDescription(), jsonCols])
        return invalidCols, jsonCols

    def plotHist(self, dfOneCol, histDict, typeHist, numBars=20, valuesBar=True):
        """
        This function builds the histogram (bins) of an categorical column dataframe.
        Inputs:
        dfOneCol: A dataFrame of one column.
        histDict: Python dictionary with histogram values
        typeHist: type of histogram to be generated, numerical or categorical
        numBars: Number of bars in histogram.
        valuesBar: If valuesBar is True, values of frequency are plotted over bars.
        Outputs: dictionary of the histogram generated
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        Example:
        self.plotHist(df[column], typeHist='categorical', valuesBar=True)
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        """

        assert isinstance(typeHist, str), "Error, typeHist argument provided must be a string."
        assert typeHist == ('categorical') or (
            typeHist == 'numerical'), "Error, typeHist only can be 'categorical' or 'numerical'."

        # Getting column of dataframe provided
        column = dfOneCol.columns[0]

        if typeHist == 'categorical':
            # Plotting histogram
            self.__plotCatHist(histDict, column, valuesBar=True)
        else:
            # Plotting histogram
            self.__plotNumHist(histDict, column, valuesBar=True)

    def getCategoricalHist(self, dfOneCol, numBars):
        """This function analyzes a dataframe of a single column (only string type columns) and
        returns a dictionary with bins and values of frequency.

        :param dfOneCol     One column dataFrame.
        :param numBars      Number of bars or histogram bins.
        :return histDict    Python dictionary with histogram values and bins."""

        assert isinstance(numBars, int), "Error, numBars argument must be a string dataType."
        assert len(dfOneCol.columns) == 1, "Error, Dataframe provided must have only one column."
        assert dfOneCol.dtypes[0][1] == 'string', "Error, Dataframe column must be string data type."

        # Column Name
        column = dfOneCol.columns[0]

        dfKeys = dfOneCol.select(column) \
            .groupBy(column).count() \
            .sort('count', ascending=False) \
            .limit(numBars)

        # Extracting information dataframe into a python dictionary:
        histDict = []
        for row in dfKeys.collect():
            histDict.append(self.__createDict(['value', 'cont'], [row[0], row[1]]))

        return histDict

    def getNumericalHist(self, dfOneCol, numBars):
        """This function analyzes a dataframe of a single column (only numerical columns) and
        returns a dictionary with bins and values of frequency."""

        assert len(dfOneCol.columns) == 1, "Error, Dataframe provided must have only one column."

        # Column Name
        column = dfOneCol.columns[0]

        tempDf = dfOneCol.withColumn(column, col(column).cast('float')).na.drop(subset=column)

        # If we obtain a null column:
        assert (type(tempDf.first()) != type(None)), \
            "Error, Make sure column dataframe has numerical features. One of the first actions \
        getNumericalHist function does is a convertion dataType from original datatype \
        to float. If the column provided has only values that are \
        not numbers parseables to float, it will flag this error."

        # Getting min and max values:
        minValue = tempDf.select(cmin(col(column))).first()[0]
        maxValue = tempDf.select(cmax(col(column))).first()[0]

        # Numero de valores entre el máximo y el minimo:
        stepsValue = (maxValue - minValue) / (numBars)

        # if stepsValue is different to zero, for example, there is only one number distinct in columnName
        if stepsValue == 0:
            binsValues = [0, maxValue]  # Only one bin is generated
        else:
            # Intervals between min an max values of columnName are made based in number of numBars wanted
            binsValues = np.arange(minValue, maxValue + stepsValue, stepsValue)

        # Valores unicos:
        uniValues = [row[0] for row in tempDf.select(column).distinct().take(numBars * 100)]

        # Si la cantidad de bins es menor que los valores unicos, entonces se toman los valores unicos como bin.
        if len(binsValues) < len(uniValues):
            binValues = uniValues

        # This function search over columnName dataFrame to which interval belongs each cell
        # It returns the columnName dataFrame with an additional columnName which describes intervals of each columnName cell.
        def generateExpr(columnName, listIntervals):
            if (len(listIntervals) == 1):
                return when(col(columnName).between(listIntervals[0][0], listIntervals[0][1]), 0).otherwise(None)
            else:
                return (when((col(columnName) >= listIntervals[0][0]) & (col(columnName) < listIntervals[0][1]),
                             len(listIntervals) - 1)
                        .otherwise(generateExpr(columnName, listIntervals[1:])))

                # +--------+--------------------+
                # |columns |Number of list pairs|
                # +--------+--------------------+
                # |       5|                   4|
                # |       3|                   7|
                # |       6|                   3|
                # |       9|                   0|
                # |       1|                   9|
                # |       6|                   3|
                # |       4|                   6|
                # +--------+--------------------+

        # Getting ranges from list: i.e. [(0,1),(1,2),(2,3),(3,4)]
        funcPairs = lambda liste: [(liste[x], liste[x + 1]) for x in range(0, len(liste) - 1)]
        ranges = funcPairs(binsValues)

        # Identifying to which group belongs each cell of column Dataframe and count them in order to get frequencies
        # for each searh interval.
        freqDf = tempDf.select(col(column), generateExpr(column, ranges).alias("value")) \
            .groupBy("value").count()

        # +-----------+-----+
        # |intervals  |count|
        # +-----------+-----+
        # |          0|    2|
        # |          1|    3|
        # |          4|    2|
        # |          5|    1|
        # |          6|    1|
        # |         10|    1|
        # +-----------+-----+

        # Reverting the order of the list ranges, because 0 in the last interval in list provided to get FreqDF
        # so it is more intuitive if the list of ranges is reverted. Then the first and second pair interval in ranges1
        # correspond to 0 and 1 interval in list
        ranges1 = list(reversed(ranges))

        # From intervals, bins are calculated as the average of min and max interval.
        bins = [np.mean([rmin, rmax]) for (rmin, rmax) in ranges1]

        func = udf(lambda x: float(bins[x]), DoubleType())

        # Setting position of bars according to bins and group intervals:
        freqDf = freqDf.na.drop().withColumn('value', func(col('value')))

        # Extracting information dataframe into a python dictionary:
        histDict = []
        for row in freqDf.collect():
            histDict.append(self.__createDict(['value', 'cont'], [row[0], row[1]]))

        return histDict

    def uniqueValuesCol(self, column):
        """This function counts the number of values that are unique and also the total number of values.
        Then, returns the values obtained.
        :param  column      Name of column dataFrame, this argument must be string type.
        :return         dictionary of values counted, as an example:
                        {'unique': 10, 'total': 15}
        """

        assert column, "Error, column name must be string type."

        total = self.__df.select(column).count()
        distincts = self.__df.select(column).distinct().count()

        return {'total': total, 'unique': distincts}

    def writeJson(self, jsonCols, pathToJsonFile):

        # assert isinstance(jsonCols, dict), "Error: columnAnalyse must be run before writeJson function."

        jsonCols = dumps(jsonCols)

        with open(pathToJsonFile, 'w') as outfile:
            # outfile.write(str(jsonCols).replace("'", "\""))
            outfile.write(jsonCols)
