from pyspark.sql import DataFrame

from optimus.dataframe.plots.functions import plot_scatterplot, plot_boxplot, plot_frequency, plot_hist, \
    plot_correlation, plot_qqplot
from optimus.helpers.columns import check_column_numbers
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import PYSPARK_NUMERIC_TYPES
from optimus.helpers.decorators import add_attr


def plot(self):
    @add_attr(plot)
    def hist(columns=None, buckets=10, output_format="plot", output_path=None):
        """
        Plot histogram
        :param columns: Columns to be printed
        :param buckets: Number of buckets
        :param output_format:
        :param output_path: path where the image is going to be saved
        :return:
        """
        columns = parse_columns(self, columns)

        data = self.cols.hist(columns, buckets)
        for col_name in data.keys():
            plot_hist({col_name: data[col_name]["hist"]}, output=output_format, path=output_path)

    @add_attr(plot)
    def scatter(columns=None, buckets=30, output_format="plot", output_path=None):
        """
        Plot boxplot
        :param columns: columns to be printed
        :param buckets: number of buckets
        :param output_format:
        :param output_path: path where the image is going to be saved
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        data = self.cols.scatter(columns, buckets)
        plot_scatterplot(data, output=output_format, path=output_path)

    @add_attr(plot)
    def box(columns=None, output_format="plot", output_path=None):
        """
        Plot boxplot
        :param columns: Columns to be printed
        :param output_format:
        :param output_path: path where the image is going to be saved
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        for col_name in columns:
            stats = self.cols.boxplot(col_name)
            plot_boxplot({col_name: stats}, output=output_format, path=output_path)

    @add_attr(plot)
    def frequency(columns=None, buckets=10, output_format="plot", output_path=None):
        """
        Plot frequency chart
        :param columns: Columns to be printed
        :param buckets: Number of buckets
        :param output_format:
        :param output_path: path where the image is going to be saved
        :return:
        """
        columns = parse_columns(self, columns)
        data = self.cols.frequency(columns, buckets)

        for k, v in data.items():
            plot_frequency({k: v}, output=output_format, path=output_path)

    @add_attr(plot)
    def correlation(col_name, method="pearson", output_format="plot", output_path=None):
        """
        Compute the correlation matrix for the input data set of Vectors using the specified method. Method
        mapped from pyspark.ml.stat.Correlation.
        :param col_name: The name of the column for which the correlation coefficient needs to be computed.
        :param method: String specifying the method to use for computing correlation. Supported: pearson (default),
        spearman.
        :param output_format: Output image format
        :param output_path: Output path
        :return: Heatmap plot of the corr matrix using seaborn.
        """
        cols_data = self.cols.correlation(col_name, method, output="array")
        plot_correlation(cols_data, output=output_format, path=output_path)

    @add_attr(plot)
    def qqplot(columns, n=100, output_format="plot", output_path=None):
        """

        :param columns:
        :param n: Sample size
        :param output_format: Output format
        :param output_path: Path to the output file
        :return:
        """
        df = self

        columns = parse_columns(self, cols_args=columns, filter_by_column_dtypes=PYSPARK_NUMERIC_TYPES)

        if columns is not None:
            sample_data = df.sample_n(n=n, random=True)
            for col_name in columns:
                plot_qqplot(col_name, sample_data, output=output_format, path=output_path)

    return plot


DataFrame.plot = property(plot)
