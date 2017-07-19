# Importing DataFrameTransformer library
from optimus.DfTransf import DataFrameTransformer
# Importing DataFrameAnalyzer library
from optimus.DfAnalizer import DataFrameAnalizer


class Optimus():
    """This class can be considered as a wrapper of DataFrameTransformer and
    DataFrameAnalyzer classes.
    """
    def __init__(self, df, pathFile, pu=1):
        """Constructor.
        In this method DataFrameTransformer and DataFrameAnalyzer classes are
        instantiated.
        """
        # Path file:
        self.__pathFile = pathFile

        # Pu file:
        self.__pu = pu

        # Instance of transformer class:
        self.transformer = DataFrameTransformer(df)

        # Instance of analyzer class:
        self.analyzer = DataFrameAnalizer(df, self.__pathFile, self.__pu)

    def getDataframe(self):
        """This function return the dataframe of the class"""
        return self.transformer.getDataframe()

    def setDataframe(self, df):
        """This function set a dataframe into the class for subsequent actions.
        """
        assert isinstance(df, pyspark.sql.dataframe.DataFrame), "Error: df argument must a sql.dataframe type"
        self.transformer.setDataframe(df)

    def __executeAnalyzer(self, columns):
        # First
        self.analyzer.unpersistDF()
        del self.analyzer
        # Instance of analyzer class:
        self.analyzer = DataFrameAnalizer(self.transformer.getDataframe(), self.__pathFile, self.__pu)
        # Sampling if it is specified (this method is depedent to the pu argument)
        self.analyzer.analyzeSample()
        # Analize column specified by user:
        self.analyzer.columnAnalize(columnList=columns,
                                    plots=True,
                                    valuesBar=False,
                                    printType=True,
                                    numBars=50)
    def trimCol(self, columns):
        """This methods cut left and right extra spaces in column strings provided by user.
        :param columns   list of column names of dataFrame.
                        If a string "*" is provided, the method
                        will do the trimming operation in whole dataFrame.

        :return transformer object
        """
        # Calling trimCol
        self.transformer.trimCol(columns=columns)
        # Execute analyzer:
        self.__executeAnalyzer(columns)

    def dropCol(self, df, columns):
        """This method eliminate the list of columns provided by user.
        :param columns      list of columns names or a string (a column name).

        :return transformer object
        """

        # Calling dropCol
        self.transformer.dropCol(columns=columns)
        # Execute analyzer:
        self.__executeAnalyzer(columns)

    def lowerCase(self, columns):
        """This function set all strings in columns of dataframe specified to lowercase.
        Columns argument must be a string or a list of string. In order to apply this
        function to all dataframe, columns must be equal to '*'"""

        # Calling lowerCase
        self.transformer.lowerCase(columns=columns)
        # Execute analyzer:
        self.__executeAnalyzer(columns)

    def upperCase(self, columns):
        """This function set all strings in columns of dataframe specified to uppercase.
        Columns argument must be a string or a list of string. In order to apply this function to all
        dataframe, columns must be equal to '*'"""

        # Calling lowerCase
        self.transformer.upperCase(columns=columns)
        # Execute analyzer:
        self.__executeAnalyzer(columns)

    def keepCol(self, columns):
        """This method keep only columns specified by user with columns argument in DataFrame.
        :param columns list of columns or a string (column name).

        :return transformer object
        """

        # Calling keepCol
        self.transformer.keepCol(columns=columns)
        # Execute analyzer:
        self.__executeAnalyzer(columns)
