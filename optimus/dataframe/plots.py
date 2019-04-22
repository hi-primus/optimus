import numpy as np
import seaborn as sns
from pyspark.sql import DataFrame

from optimus import PYSPARK_NUMERIC_TYPES
from optimus.functions import plot_hist, plot_freq, plot_boxplot, plot_scatterplot
from optimus.helpers.decorators import add_attr
from optimus.helpers.functions import parse_columns


def plot(self):
    @add_attr(plot)
    def hist(columns=None, buckets=10):
        """
        Plot histogram
        :param columns: Columns to be printed
        :param buckets: Number of buckets
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)

        for col_name in columns:
            data = self.cols.hist(col_name, buckets)
            plot_hist({col_name: data})

    @add_attr(plot)
    def scatterplot(columns=None, buckets=30):
        """
        Plot boxplot
        :param columns: columns to be printed
        :param buckets: number of buckets
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)

        data = self.cols.scatterplot(columns, buckets)
        plot_scatterplot(data)

    @add_attr(plot)
    def boxplot(columns=None):
        """
        Plot boxplot
        :param columns: Columns to be printed
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)

        for col_name in columns:
            stats = self.cols.boxplot(col_name)
            plot_boxplot({col_name: stats})

    @add_attr(plot)
    def frequency(columns=None, buckets=10):
        """
        Plot frequency chart
        :param columns: Columns to be printed
        :param buckets: Number of buckets
        :return:
        """
        columns = parse_columns(self, columns)

        for col_name in columns:
            data = self.cols.frequency(col_name, buckets)
            plot_freq(data)

    @add_attr(plot)
    def correlation(vec_col, strategy="mean", method="pearson"):
        """
        Compute the correlation matrix for the input dataset of Vectors using the specified method. Method
        mapped from  pyspark.ml.stat.Correlation.
        :param vec_col: The name of the column of vectors for which the correlation coefficient needs to be computed.
        :param strategy:
        This must be a column of the dataset, and it must contain Vector objects.
        :param method: String specifying the method to use for computing correlation. Supported: pearson (default),
        spearman.
        :return: Heatmap plot of the corr matrix using seaborn.
        """

        corr = self.correlation(vec_col, method, strategy, output="array")
        return sns.heatmap(corr, mask=np.zeros_like(corr, dtype=np.bool), cmap=sns.diverging_palette(220, 10,
                                                                                                     as_cmap=True))

    return plot


DataFrame.plot = property(plot)
