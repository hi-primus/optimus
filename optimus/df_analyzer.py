# -*- coding: utf-8 -*-
# Importing sql functions
from pyspark.sql.functions import udf, when, col, min as cmin, max as cmax
from pyspark.sql.types import StringType, DoubleType
# Import stat functions
from pyspark.ml.stat import Correlation
# Importing plotting libraries
import matplotlib.pyplot as plt
import seaborn as sns
# Importing numeric module
import numpy as np
# Importing features to build tables
from IPython.display import display
# Importing time libraries to measure time
import time
# Importing dumps
from json import dumps
import pyspark.sql.dataframe

# Import profiler
import spark_df_profiling_optimus


class ColumnTables:
    """This class builds a table to describe the number of the different dataTypes in a column dataFrame.

    It is important to notice that this is not the best way to build a table. It will be better if a general
    building table class is built"""

    def __init__(self, col_name, data_type_inferred, qtys, percents):
        self.qtys = qtys
        self.percents = percents
        self.col_name = col_name
        self.data_type_inferred = data_type_inferred
        self.labels_table = ["None", "Empty str", "String", "Integer", "Float"]

    def _repr_html_(self):
        # Creation of blank table:
        html = ["<table width=50%>"]

        # Adding a row:
        html.append("<tr>")
        html.append("<td colspan=3 >{0}</td>".format("<b> Column name: </b>" + self.col_name))
        html.append("</tr>")

        # Adding a row:
        html.append("<tr>")
        html.append("<td colspan=3 >{0}</td>".format("<b> Column datatype: </b>" + self.data_type_inferred))
        html.append("</tr>")

        # Adding a row:
        html.append("<tr>")
        html.append("<th>{0}</td>".format('Datatype'))
        html.append("<th>{0}</td>".format('Quantity'))
        html.append("<th>{0}</td>".format('Percentage'))
        html.append("</tr>")

        for ind in range(len(self.labels_table)):
            html.append("<tr>")
            html.append("<td>{0}</td>".format(self.labels_table[ind]))
            html.append("<td>{0}</td>".format(self.qtys[ind + 1]))
            html.append("<td>{0}</td>".format("%0.2f" % self.percents[ind] + " %"))
            html.append("</tr>")

        html.append("</table>")
        return ''.join(html)


# Input data:
class GeneralDescripTable:
    def __init__(self, file_name, column_number, row_number):
        self.labels = ['File Name', "Columns", "Rows"]
        self.dat = [file_name, column_number, row_number]

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


class DataTypeTable:
    def __init__(self, lis):
        self.lis = lis
        self.html = ""

    @classmethod
    def col_table(cls, lis):
        html = []
        html.append("<table width=100%>")
        for x in lis:
            html.append("<tr >")
            html.append("<td style='text-align: center'>")

            html.append("<div style='min-height: 20px;'>")
            html.append(str(x))
            html.append("</div>")
            html.append("</td>")
            html.append("</tr>")
        html.append("</table>")
        return "".join(html)

    @property
    def _repr_html_(self):
        self.html = ["<table width=50%>"]
        headers = ['integers', 'floats', 'strings']

        # This verify is some of list of types is empty.
        # The idea is not to draw an empty table.
        ver = [x != [] for x in self.lis]

        # First row of table:
        self.html.append("<tr>")
        for x, _ in enumerate(headers):
            if ver[x]:  # If list is not empty, print its head
                self.html.append("<th style='text-align: center'>")
                self.html.append(str(headers[x]))
                self.html.append("</th>")
        self.html.append("</tr>")

        # Adding the second row:
        self.html.append("<tr>")
        for x in range(len(self.lis)):
            if ver[x]:
                # Adding a td
                self.html.append("<td style='vertical-align: top;text-align: center;'>")
                # Adding a table:


                self.html.append(self.col_table(self.lis[x]))

                self.html.append("</td>")
        self.html.append("</tr>")

        self.html.append("</table>")
        return ''.join(self.html)


# This class makes a profile for a given dataframe and its different general features.
# Based on spark-df-profiling
class DataFrameProfiler:
    def __init__(self, df):
        # Asserting if df is dataFrame datatype.
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), \
            "Error, df argument must be a pyspark.sql.dataframe.DataFrame instance"

        # Dataframe
        self._df = df
        self._df.cache()

    def profiler(self):
        """
        This function calls the ProfileReport method from spark-df-profiling-optimus,
        it gets the current DF in the analyzer and them returns the HTML profile"
        :return: Profile of the DF in HTML format embedded in the Notebook
        """
        df_profiler = self._df
        return spark_df_profiling_optimus.ProfileReport(df_profiler)

    def show(self, n=10):
        """This function shows the dataframe of the class
        :param n: number or rows to show
        :rtype: pyspark.sql.dataframe.DataFrame.show()
        """
        return self._df.show(n)


# This class makes an analysis of dataframe datatypes and its different general features.
class DataFrameAnalyzer:
    def __init__(self, df, path_file=None, pu=0.1, seed=13):
        # Asserting if df is dataFrame datatype.
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), \
            "Error, df argument must be a pyspark.sql.dataframe.DataFrame instance"
        # Asserting if path specified is string datatype
        assert isinstance(path_file, (str, type(None))), \
            "Error, path_file argument must be string datatype or NoneType."
        # Asserting if path includes the type of filesystem
        if not isinstance(path_file, type(None)):
            assert (("file:///" == path_file[0:8]) or ("hdfs:///" == path_file[0:8])), \
                "Error: path must be with a 'file://' prefix \
                if the file is in the local disk or a 'path://' \
                prefix if the file is in the Hadood file system"

        # Asserting if seed is integer
        assert isinstance(seed, int), "Error, seed must be an integer"

        # Asserting pu is between 0 and 1.
        assert (pu <= 1) and (pu >= 0), "Error, pu argument must be between 0 and 1"

        # Dataframe
        self._row_number = 0
        if path_file is not None:
            self._path_file = path_file
        elif path_file is None:
            self._path_file = None
        self._currently_observed = 0  # 0 => whole 1 => partition
        self._df = df
        self._df.cache()
        self._sample = df.sample(False, pu, seed)

    @classmethod
    def _create_dict(cls, keys, values):
        """This functions is a helper to build dictionaries. The first argument must be a list of keys but it
        can be a string also (in this case the string will be packaged into a list. The keys provided
        will be the dictionary keys. The second argument represents the values of each key. values argument can
        be a list or another dictionary."""

        dicc = {}
        # Assert if keys is a string, if not, the string is placed it inside a list
        if isinstance(keys, str):
            keys = [keys]
        # If values is not a list, place it inside a list also
        if not isinstance(values, list):
            values = [values]
        # Making dictionary
        for index, _ in enumerate(keys):
            dicc[keys[index]] = values[index]
        # Return dictionary built
        return dicc

    @classmethod
    def _verification(cls, temp_df, column_name):
        # Function for determine if register value is float or int or string:
        def data_type(value):
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

        func = udf(data_type, StringType())
        temp_df = temp_df.withColumn('types', func(col(column_name)))

        types = temp_df.groupBy('types').count()

        typeslabels = ['integer', 'float', 'string', 'null']

        numbers = {}
        for row in types.collect(): numbers[row[0]] = row[1]

        # completing values of rest types:
        for label in typeslabels:
            if not label in numbers:
                numbers[label] = 0

        number_empty_strs = temp_df.where(col(column_name) == '').count()
        numbers['string'] = numbers['string'] - number_empty_strs

        # List of returning values:
        values = [temp_df, numbers['null'], number_empty_strs, numbers['string'], numbers['integer'], numbers['float']]
        return values

        # Analize of each column:

    def _analyze(self, df_col_analyzer, column, row_number, plots, print_type, values_bar, num_bars, types_dict):
        t = time.time()
        sample_table_dict = {'string': 0., 'integer': 0, 'float': 0}
        # Calling verification ruotine to obtain datatype's counts
        # returns: [dataframeColumn, number of nulls, number of strings, number of integers, number of floats]
        dtype_numbers = self._verification(df_col_analyzer.select(column), column)

        def valid_col_check(f):
            """This functions analyze if column has more than two different data types
            and return True if the column analyzed is found to be with different datatypes"""
            # Verifying if there is a dummy column:
            dummy = any(x == 1 for x in f[0:2]) and all(x == 0 for x in f[2:])

            # Verifying if there is a column with different datatypes:
            # Sum the first and the second number:
            sum_first_and_second = lambda lis: [lis[x] + lis[0] if x == 1 else lis[x] for x in
                                                range(len(lis))][
                                               1:]
            # Check if the column has different datatypes:
            different_types = sum([1 if x == 0 else 0 for x in sum_first_and_second(f[1:])]) < 2

            return dummy or different_types

        # Calculate percentages of datatypes:
        percentages = [(dtypeNumber / float(row_number)) * 100 for dtypeNumber in dtype_numbers[1:]]

        if valid_col_check(dtype_numbers[1:]):
            invalid_cols = column
        else:
            invalid_cols = False

        # Instance of columnTables to display results:
        type_col = df_col_analyzer.select(column).dtypes[0][1]

        temp_obj = ColumnTables(column, type_col, dtype_numbers, percentages)
        display(temp_obj)

        if type_col != 'string':
            print("Min value: ", df_col_analyzer.select(cmin(col(column))).first()[0])
            print("Max value: ", df_col_analyzer.select(cmax(col(column))).first()[0])

        # Plot bar stack:
        # if plots==True: self.__bar_stack_type(percentages, column)

        if print_type:
            typeslabels = ['integer', 'float', 'string']
            list_of_eachtype = [dtype_numbers[0].where(col('types') == tipo).where(col(column) != '') \
                                    .drop('types').select(column).distinct() \
                                    .limit(num_bars).rdd.map(lambda x: x[column]).collect() for tipo in typeslabels]
            display(DataTypeTable(list_of_eachtype))
            sample_table_dict = self._create_dict(typeslabels, list_of_eachtype)

        # Obtaining number of strings and numbers to decide what type of histogram (numerical
        # or categorical is needed)
        string_qty = dtype_numbers[3]
        number_qty = np.sum(dtype_numbers[4:])

        # Plotting histograms:
        # If number of strings is greater than (number of integers + number of floats)
        if string_qty > number_qty:
            # Building histogram:
            hist_dict = self.get_categorical_hist(df_col_analyzer.select(column), num_bars)
            hist_plot = {"data": hist_dict}

            if plots:
                self.plot_hist(
                    column=column,
                    type_hist='categorical',
                    values_bar=values_bar)
                plt.show()
        elif string_qty < number_qty:
            # Building histogram:
            hist_dict = self.get_numerical_hist(df_col_analyzer.select(column), num_bars)
            hist_plot = {"data": hist_dict}

            if plots:
                # Create the general blog and the "subplots" i.e. the bars
                self.plot_hist(column=column,
                               type_hist='numerical',
                               values_bar=values_bar)
                plt.show()

        else:
            print("No valid data to print histogram or plots argument set to False")

            hist_plot = {"data": [{"count": 0, "value": "none"}]}

        numbers = list(dtype_numbers[1:])
        valid_values = self._create_dict(['total', 'string', 'integer', 'float'],
                                         [int(np.sum(dtype_numbers[-3:])),
                                          numbers[-3],
                                          numbers[-2],
                                          numbers[-1]]
                                         )

        missing_values = self._create_dict(
            ['total', 'empty', 'null'],
            [int(np.sum(numbers[0:2])), numbers[1], numbers[0]
             ])

        # returns: [number of nulls, number of strings, number of integers, number of floats]
        summary_dict = self._create_dict(
            ["name", "type", "total", "valid_values", "missing_values"],
            [column, types_dict[type_col], row_number, valid_values, missing_values
             ])

        column_dict = self._create_dict(
            ["summary", "graph", "sample"],
            [summary_dict, hist_plot, sample_table_dict]
        )

        print("end of __analyze", time.time() - t)
        return invalid_cols, percentages, numbers, column_dict

    # This function, place values of frequency in histogram bars.
    @classmethod
    def _values_on_bar(cls, plot_fig):
        rects = plot_fig.patches
        for rect in rects:
            # Getting height of bars:
            height = rect.get_height()
            # Plotting texts on bars:
            plt.text(rect.get_x() + rect.get_width() / 2.,
                     1.001 * height, "{}".format(height),
                     va='bottom', rotation=90)

    def _plot_num_hist(self, hist_dict, column, values_bar):
        values = [list(lis) for lis in list(zip(*[(dic['value'], dic['cont']) for dic in hist_dict]))]

        bins = values[0]

        if len(bins) == 1:
            width = bins[0] * 0.3
            bins[0] -= width
        else:
            width = min(abs(np.diff(bins))) * 0.15

        # Plotting histogram:
        plot_fig = plt.bar(np.array(bins) - width, values[1], width=width)

        if values_bar: self._values_on_bar(plot_fig)

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

    def _plot_cat_hist(self, hist_dict, column, values_bar):
        # Extracting values from dictionary
        k = list(filter(lambda k: k != 'cont', hist_dict[0].keys()))[0]

        values = [list(lis) for lis in list(zip(*[(dic[k], dic['cont']) for dic in hist_dict]))]
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
        plot_fig = plt.bar(bins, values[1], width=width)

        # If user want to see values of frequencies over each bar
        if values_bar: self._values_on_bar(plot_fig)

        # Plot Title:
        plt.title(column)
        # Limits Y axes
        plt.ylim([0, np.max(values[1]) * 1.3])
        plt.xlim([-1, index[-1] + 1])
        plt.show()

    def _swap_status(self):
        self._exchange_data()  # exchange data
        self._currently_observed = 0 if self._currently_observed == 1 else 1

    def _exchange_data(self):  # Swaps the data among DF and the Sample
        if self._currently_observed == 0:
            self._hidden_data = self._df
            self._df = self._sample

        else:
            self._df = self._hidden_data

    def analyze_sample(self):
        if self._currently_observed == 0:
            self._swap_status()

    def analyze_complete_data(self):
        if self._currently_observed == 1:
            self._swap_status()

    def unpersist_df(self):
        self._df.unpersist()

    def set_data_frame(self, df):
        """This function set a dataframe into the class for subsequent actions.
        """
        self._df = df

    @property
    def get_data_frame(self):
        """This function return the dataframe of the class"""
        return self._df

    def show(self, n=10, truncate=True):
        """This function shows the dataframe of the class
        :param n: number or rows to show
        :param truncate: If set to True, truncate strings longer than 20 chars by default.
        :rtype: pyspark.sql.dataframe.DataFrame.show()
        """
        return self._df.show(n, truncate)

    # Function to give general features of dataFrame:
    def general_description(self):
        # General data description:
        # Filename:
        if self._path_file is None:
            file_name = "file with no path"
        else:
            file_name = self._path_file.split('/')[-1]
        # Counting the number of columns:
        row_number = self._df.count()
        # Counting the number of rows:
        col_number = len(self._df.columns)
        # Writting General Description table:
        temp_obj = GeneralDescripTable(file_name, col_number, row_number)
        display(temp_obj)

        data_types = len(set([x[1] for x in self._df.dtypes]))

        return self._create_dict(["filename", "size", "columns", "rows", "datatypes"],
                                 [file_name, 500, col_number, row_number, data_types])

    # Funtion to analize column datatypes, it also plot proportion datatypes and histograms:
    def column_analyze(self, column_list, plots=True, values_bar=True, print_type=False, num_bars=10, print_all=False):
        """
        # This function counts the number of registers in a column that are numbers (integers, floats) and the number of
        # string registers.
        # Input:
        # column_list: a list or a string column name
        # values_bar (optional): Can be True or False. If it is True, frequency values are placed over each bar.
        # print_type (optional): Can be True or False. If true it will print the analysis.
        # num_bars: number of bars printed in histogram
        # Output:
        # values: a list containing the number of the different datatypes [nulls, strings, integers, floats]
        """
        # Asserting data variable column_list:
        assert isinstance(column_list, (str, list)), "Error: column_list has to be a string or list."

        # Asserting if values_bar is type Boolean
        assert isinstance(values_bar, bool), "Error: values_bar must be boolean, True or False."

        # Asserting if print_type is type Boolean
        assert isinstance(print_type, bool), "Error: print_type must be boolean. True or False."

        # Counting
        time1 = time.time()

        df_col_analyzer = self._df

        # Column assignation:
        columns = []

        # If column_list is a string, convert it in a list:
        if isinstance(column_list, str):
            if column_list == "*":
                columns = df_col_analyzer.columns
            else:
                columns = [column_list]

        else:
            if len(column_list) > 1:
                columns = column_list

        # Asserting if columns provided are in dataFrame:
        assert all(
            col in df_col_analyzer.columns for col in
            columns), 'Error: Columns or column does not exist in the dataFrame'

        types = {"string": "ABC", "int": "#", "integer": "#", "float": "##.#", "double": "##.#", "bigint": "#"}

        row_number = self._df.count()

        invalid_cols = []
        json_cols = []
        # In case first is not set, this will analyze all columns
        for column in columns:
            # Calling function analyze:
            inv_col, _, _, d = self._analyze(
                df_col_analyzer.select(column),
                column,
                row_number,
                plots,
                print_type,
                values_bar=values_bar,
                num_bars=num_bars,
                types_dict=types)

            # Save the invalid col if exists
            invalid_cols.append(inv_col)

            json_cols.append(d)

        time2 = time.time()
        print("Total execution time: ", (time2 - time1))

        invalid_cols = list(filter(lambda x: x != False, invalid_cols))

        json_cols = self._create_dict(["summary", "columns"], [self.general_description(), json_cols])
        if print_all:
            return invalid_cols, json_cols

    def plot_hist(self, column, type_hist, values_bar=True):
        """
        This function builds the histogram (bins) of an categorical column dataframe.
        Inputs:
        df_one_col: A dataFrame of one column.
        hist_dict: Python dictionary with histogram values
        type_hist: type of histogram to be generated, numerical or categorical
        values_bar: If values_bar is True, values of frequency are plotted over bars.
        Outputs: dictionary of the histogram generated
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        Example:
        self.plotHist(df[column], type_hist='categorical', values_bar=True)
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        """

        assert isinstance(type_hist, str), "Error, type_hist argument provided must be a string."
        assert type_hist == 'categorical' or (
            type_hist == 'numerical'), "Error, type_hist only can be 'categorical' or 'numerical'."

        df_one_col = self._df.select(column)

        # Getting column of dataframe provided
        column_plot = df_one_col.columns[0]

        if type_hist == 'categorical':
            # Plotting histogram
            hist_dict = self.get_categorical_hist(df_one_col, num_bars=10)
            self._plot_cat_hist(hist_dict, column_plot, values_bar)
        else:
            # Plotting histogram
            hist_dict = self.get_numerical_hist(df_one_col, num_bars=10)
            self._plot_num_hist(hist_dict, column_plot, values_bar)

    def get_categorical_hist(self, df_one_col, num_bars):
        """This function analyzes a dataframe of a single column (only string type columns) and
        returns a dictionary with bins and values of frequency.

        :param df_one_col     One column dataFrame.
        :param num_bars      Number of bars or histogram bins.
        :return hist_dict    Python dictionary with histogram values and bins."""

        assert isinstance(num_bars, int), "Error, num_bars argument must be a string dataType."
        assert len(df_one_col.columns) == 1, "Error, Dataframe provided must have only one column."
        assert df_one_col.dtypes[0][1] == 'string', "Error, Dataframe column must be string data type."

        # Column Name
        column = df_one_col.columns[0]

        df_keys = df_one_col.select(column) \
            .groupBy(column).count() \
            .sort('count', ascending=False) \
            .limit(num_bars)

        # Extracting information dataframe into a python dictionary:
        hist_dict = []
        for row in df_keys.collect():
            hist_dict.append(self._create_dict(['value', 'cont'], [row[0], row[1]]))

        return hist_dict

    def get_numerical_hist(self, df_one_col, num_bars):
        """This function analyzes a dataframe of a single column (only numerical columns) and
        returns a dictionary with bins and values of frequency."""

        assert len(df_one_col.columns) == 1, "Error, Dataframe provided must have only one column."

        # Column Name
        column = df_one_col.columns[0]

        temp_df = df_one_col.withColumn(column, col(column).cast('float')).na.drop(subset=column)

        # If we obtain a null column:
        assert not isinstance(temp_df.first(), type(None)), \
            "Error, Make sure column dataframe has numerical features. One of the first actions \
        getNumericalHist function does is a change dataType from original datatype \
        to float. If the column provided has only values that are \
        not numbers parseables to float, it will flag this error."

        # Getting min and max values:
        min_value = temp_df.select(cmin(col(column))).first()[0]
        max_value = temp_df.select(cmax(col(column))).first()[0]

        # Numero de valores entre el máximo y el minimo:
        steps_value = (max_value - min_value) / num_bars

        # if stepsValue is different to zero, for example, there is only one number distinct in columnName
        if steps_value == 0:
            bins_values = [0, max_value]  # Only one bin is generated
        else:
            # Intervals between min an max values of columnName are made based in number of num_bars wanted
            bins_values = np.arange(min_value, max_value + steps_value, steps_value)

        # Valores unicos:
        uni_values = [row[0] for row in temp_df.select(column).distinct().take(num_bars * 100)]

        # Si la cantidad de bins es menor que los valores unicos, entonces se toman los valores unicos como bin.
        if len(bins_values) < len(uni_values):
            bins_values = uni_values

        # This function search over columnName dataFrame to which interval belongs each cell
        # It returns the columnName dataFrame with an additional columnName which describes intervals of each columnName cell.
        def generate_expr(column_name, list_intervals):
            if len(list_intervals) == 1:
                return when(col(column_name).between(list_intervals[0][0], list_intervals[0][1]), 0).otherwise(None)
            else:
                return (when((col(column_name) >= list_intervals[0][0]) & (col(column_name) < list_intervals[0][1]),
                             len(list_intervals) - 1)
                        .otherwise(generate_expr(column_name, list_intervals[1:])))

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
        func_pairs = lambda liste: [(liste[x], liste[x + 1]) for x in range(0, len(liste) - 1)]
        ranges = func_pairs(bins_values)

        # Identifying to which group belongs each cell of column Dataframe and count them in order to get frequencies
        # for each searh interval.
        freq_df = temp_df.select(col(column), generate_expr(column, ranges).alias("value")) \
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
        freq_df = freq_df.na.drop().withColumn('value', func(col('value')))

        # Extracting information dataframe into a python dictionary:
        hist_dict = []
        for row in freq_df.collect():
            hist_dict.append(self._create_dict(['value', 'cont'], [row[0], row[1]]))

        return hist_dict

    def unique_values_col(self, column):
        """This function counts the number of values that are unique and also the total number of values.
        Then, returns the values obtained.
        :param  column      Name of column dataFrame, this argument must be string type.
        :return         dictionary of values counted, as an example:
                        {'unique': 10, 'total': 15}
        """

        assert column, "Error, column name must be string type."

        total = self._df.select(column).count()
        distincts = self._df.select(column).distinct().count()

        return {'total': total, 'unique': distincts}

    def correlation(self, vec_col, method="pearson"):
        """
        Compute the correlation matrix for the input dataset of Vectors using the specified method. Method
        mapped from  pyspark.ml.stat.Correlation.

        :param vec_col: The name of the column of vectors for which the correlation coefficient needs to be computed.
        This must be a column of the dataset, and it must contain Vector objects.
        :param method: String specifying the method to use for computing correlation. Supported: pearson (default),
        spearman.
        :return: Heatmap plot of the corr matrix using seaborn.
        """

        assert isinstance(method, str), "Error, method argument provided must be a string."

        assert method == 'pearson' or (
            method == 'spearman'), "Error, method only can be 'pearson' or 'sepearman'."

        cor = Correlation.corr(self._df, vec_col, method).head()[0].toArray()
        return sns.heatmap(cor, mask=np.zeros_like(cor, dtype=np.bool), cmap=sns.diverging_palette(220, 10,
                                                                                                   as_cmap=True))

    @classmethod
    def write_json(cls, json_cols, path_to_json_file):

        # assert isinstance(json_cols, dict), "Error: columnAnalyse must be run before writeJson function."

        json_cols = dumps(json_cols)

        with open(path_to_json_file, 'w') as outfile:
            # outfile.write(str(json_cols).replace("'", "\""))
            outfile.write(json_cols)

    def display_optimus(self):
        """
        Lets you visualize your Spark object in different ways: table, charts, maps, etc.
        :param df: spark dataframe to be analyzed
        :return: Amazing visualizations for your spark dataframes.
        """
        # Import pixiedust-optimus
        from pixiedust_optimus.display import display
        display(self._df)

    def get_frequency(self, columns, sort_by_count=True):
        """
        Get frequencies for values inside columns.

        :param columns: String or List of columns to analyze
        :param sort_by_count: Boolean if true the counts will be sort desc.
        :return: Dataframe with counts per existing values in each column.
        """
        # Asserting data variable is string or list:
        assert isinstance(columns, (str, list)), "Error: Column argument must be a string or a list."

        # If None or [] is provided with column parameter:
        assert columns != [], "Error: Column can not be a empty list []"

        # Columns
        if isinstance(columns, str):
            columns = [columns]

        # Columns
        assert all(col in self._df.columns for col in columns), \
            'Error: Columns or column does not exist in the dataFrame'

        def frequency(columns, sort_by_count):
            if sort_by_count:
                for column in columns:
                    freq = (self._df
                            .groupBy(column)
                            .count()
                            .orderBy("count", ascending=False))

                    if freq.where("count > 1").count() == 0:
                        print("No values to group in column", column)
                    else:
                        freq.show()
            else:
                for column in columns:
                    freq = (self._df
                            .groupBy(column)
                            .count())

                    if freq.where("count > 1").count() == 0:
                        print("No values to group in column", column)
                    else:
                        freq.show()

        return frequency(columns, sort_by_count)

