# Importing DataFrameTransformer library
from optimus.df_transformer import DataFrameTransformer
# Importing DataFrameAnalyzer library
from optimus.df_analyzer import DataFrameAnalyzer
from pyspark.sql.dataframe import DataFrame


class Optimus:
    """This class can be considered as a wrapper of DataFrameTransformer and
    DataFrameAnalyzer classes.
    """
    def __init__(self, df, path_file, pu=1):
        """Constructor.
        In this method DataFrameTransformer and DataFrameAnalyzer classes are
        instantiated.
        """
        # Path file:
        self._path_file = path_file

        # Pu file:
        self._pu = pu

        # Instance of transformer class:
        self.transformer = DataFrameTransformer(df)

        # Instance of analyzer class:
        self.analyzer = DataFrameAnalyzer(df, self._path_file, self._pu)

    def get_data_frame(self):
        """This function return the dataframe of the class"""
        return self.transformer.get_data_frame

    def set_data_frame(self, df):
        """This function set a dataframe into the class for subsequent actions.
        """
        assert isinstance(df, DataFrame), "Error: df argument must a pyspark.sql.dataframe.DataFrame type"
        self.transformer.set_data_frame(df)

    def _execute_analyzer(self, columns):
        # First
        self.analyzer.unpersist_df()
        del self.analyzer
        # Instance of analyzer class:
        self.analyzer = DataFrameAnalyzer(self.transformer.get_data_frame(), self._path_file, self._pu)
        # Sampling if it is specified (this method is dependent to the pu argument)
        self.analyzer.analyze_sample()
        # Analyze column specified by user:
        self.analyzer.column_analyze(column_list=columns,
                                     plots=True,
                                     values_bar=False,
                                     print_type=True,
                                     num_bars=50)

    def trim_col(self, columns):
        """This methods cut left and right extra spaces in column strings provided by user.
        :param columns   list of column names of dataFrame.
                        If a string "*" is provided, the method
                        will do the trimming operation in whole dataFrame.

        :return transformer object
        """
        # Calling trimCol
        self.transformer.trim_col(columns=columns)
        # Execute analyzer:
        self._execute_analyzer(columns)

    def drop_col(self, columns):
        """This method eliminate the list of columns provided by user.
        :param columns      list of columns names or a string (a column name).

        :return transformer object
        """

        # Calling dropCol
        self.transformer.drop_col(columns=columns)
        # Execute analyzer:
        self._execute_analyzer(columns)

    def lower_case(self, columns):
        """This function set all strings in columns of dataframe specified to lowercase.
        Columns argument must be a string or a list of string. In order to apply this
        function to all dataframe, columns must be equal to '*'"""

        # Calling lowerCase
        self.transformer.lower_case(columns=columns)
        # Execute analyzer:
        self._execute_analyzer(columns)

    def upper_case(self, columns):
        """This function set all strings in columns of dataframe specified to uppercase.
        Columns argument must be a string or a list of string. In order to apply this function to all
        dataframe, columns must be equal to '*'"""

        # Calling lowerCase
        self.transformer.upper_case(columns=columns)
        # Execute analyzer:
        self._execute_analyzer(columns)

    def keep_col(self, columns):
        """This method keep only columns specified by user with columns argument in DataFrame.
        :param columns list of columns or a string (column name).

        :return transformer object
        """

        # Calling keepCol
        self.transformer.keep_col(columns=columns)
        # Execute analyzer:
        self._execute_analyzer(columns)
