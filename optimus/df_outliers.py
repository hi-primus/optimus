from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import abs as absspark


class OutlierDetector:
    """
    Outlier detection for pyspark dataframes.
    """
    def __init__(self, df, column, threshold=2):
        """
        :param df: Spark Dataframe to analyze
        :param column: Column in dataframe to get outliers
        :param threshold: Threshold for MAD. Default = 2.
        """
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        self._df = df
        self._column = column

        self.median_value = median(self._df, self._column)

        absolute_deviation = (self._df
                             .select(self._column)
                             .orderBy(self._column)
                             .withColumn(self._column, absspark(col(self._column) - self.median_value))
                             .cache())

        self.mad_value = median(absolute_deviation, column)

        self.threshold = threshold

        self._limits = []
        self._limits.append(round((self.median_value - self.threshold * self.mad_value), 2))
        self._limits.append(round((self.median_value + self.threshold * self.mad_value), 2))

    def run(self):
        """
        Get list of values within accepted range, without duplicates
        """

        limits = self._limits
        column = self._column

        values_within_range = list(set((self._df
                                      .rdd.map(lambda x: x[column])
                                      .filter(lambda x: x >= limits[0] and x <= limits[1])
                                      .collect())))

        return values_within_range

    def outliers(self):
        """
        Get list of values within accepted range, without duplicates
        """

        limits = self._limits
        column = self._column

        values_without_range = list(set((self._df
                                       .rdd.map(lambda x: x[column])
                                       .filter(lambda x: x < limits[0] or x > limits[1])
                                       .collect())))

        return values_without_range

    def delete_outliers(self):
        """
        Deletes all rows where values in the column are outliers
        """

        limits = self._limits
        column = self._column

        func = lambda x: (x >= limits[0]) & (x <= limits[1])
        self._df = self._df.filter(func(col(column)))

        return self._df

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

        assert isinstance(path_name, str), "Error: path_name argument must be a string."

        assert header == "true" or header == "false", "Error header must be 'true' or 'false'."

        if header == 'true':
            header = True
        else:
            header = False

        return self._df.write.options(header=header).mode(mode).csv(path_name, sep=sep, *args, **kargs)


def median(df, column):
    return df.approxQuantile(column, [0.5], 0.01)[0]
