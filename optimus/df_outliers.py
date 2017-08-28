from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, abs


class OutlierDetector:
    """
    Outlier detection for pyspark dataframes.
    """
    def __init__(self, df, column):
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        self._df = df
        self._column = column

        self.medianValue = self.median(self._df, self._column)

        absolute_deviation = (self._df
                             .select(self._column)
                             .orderBy(self._column)
                             .withColumn(self._column, abs(col(self._column) - self.medianValue))
                             .cache())

        self.madValue = self.median(absolute_deviation, column)

        self.threshold = 2

        self._limits = []
        self._limits.append(round((self.medianValue - self.threshold * self.madValue), 2))
        self._limits.append(round((self.medianValue + self.threshold * self.madValue), 2))

    def median(self, df, column):
        df.registerTempTable("table")

        return (self.spark
                .sql("SELECT \
                        ROUND( \
                        PERCENTILE_APPROX(" + column + ", 0.5), 2) AS " + column + " FROM table")
                .rdd.map(lambda x: x[column])
                .first())

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
