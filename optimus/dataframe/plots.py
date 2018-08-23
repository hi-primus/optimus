import numpy as np
import seaborn as sns
from pyspark.sql import DataFrame

from optimus.functions import plot_hist, plot_freq
from optimus.helpers.decorators import add_attr
from optimus.helpers.functions import parse_columns


def plots(self):
    @add_attr(plots)
    def hist(columns=None, buckets=10):
        """
        Plot histogram
        :param columns: Columns to be printed
        :param buckets: Number of buckets
        :return:
        """
        columns = parse_columns(self, columns)

        for col_name in columns:
            data = self.cols.hist(col_name, buckets)
            plot_hist({col_name: data}, output="image")

    @add_attr(plots)
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
            plot_freq({col_name: data}, output="image")

    @add_attr(plots)
    def correlation(vec_col, method="pearson"):
        """
        Compute the correlation matrix for the input dataset of Vectors using the specified method. Method
        mapped from  pyspark.ml.stat.Correlation.
        :param vec_col: The name of the column of vectors for which the correlation coefficient needs to be computed.
        This must be a column of the dataset, and it must contain Vector objects.
        :param method: String specifying the method to use for computing correlation. Supported: pearson (default),
        spearman.
        :return: Heatmap plot of the corr matrix using seaborn.
        """

        corr = self.correlation(vec_col, method, output="array")
        return sns.heatmap(corr, mask=np.zeros_like(corr, dtype=np.bool), cmap=sns.diverging_palette(220, 10,
                                                                                                     as_cmap=True))

    return plots


DataFrame.plots = property(plots)
